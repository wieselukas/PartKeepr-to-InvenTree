#!/usr/bin/python3

import requests
import json
import pprint
import getopt
import sys
import time
import shutil
import tempfile
import os
import datetime
import re

from inventree.api import InvenTreeAPI
from inventree.part import PartCategory, Part
from inventree.stock import StockItem, StockLocation
from inventree.company import Company, ManufacturerPart, SupplierPart, SupplierPriceBreak



DEFAULT_PARTKEEPR = "https://admin:password@partkeepr.ibr.cs.tu-bs.de"
DEFAULT_INVENTREE = "http://admin:password@inventree.ibr.cs.tu-bs.de:1337"
DEFAULT_CURRENCY = "EUR"

verbose = False



def getFromPartkeepr(url, base, auth):
    sep = '?'
    if('?' in url):
        sep='&'

    full_url = f'{base}{url}{sep}itemsPerPage=100000'
    full_url = full_url.replace('/partkeepr/partkeepr','/partkeepr')

    r = requests.get(full_url, auth=auth)

    if (r.status_code == 200):
        return r.json()["hydra:member"]
    return None



def getImageFromPartkeepr(url, base, auth, filename="image"):

    full_url = f'{base}{url}/getImage'
    full_url = full_url.replace('/partkeepr/partkeepr','/partkeepr')
    r = requests.get(full_url, auth=auth, stream=True)

    if (r.status_code == 200):
        r.raw.decode_content = True
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix="f-%s" % (filename))
        shutil.copyfileobj(r.raw, tmp)
        return tmp.name
    return None



def getFileFromPartkeepr(url, base, auth, filename="file"):

    full_url = f'{base}{url}/getFile'
    full_url = full_url.replace('/partkeepr/partkeepr','/partkeepr')
    r = requests.get(full_url, auth=auth, stream=True)

    if (r.status_code == 200):
        r.raw.decode_content = True
        path = f'/tmp/{filename}'
        f = open(path, 'wb')
        shutil.copyfileobj(r.raw, f)
        f.close()
        return path
    return None



def create(cls, inventree, attributes):
    n = 0
    while True:
        try:
            c = cls.create(inventree, attributes)
            return c
        except Exception as err:
            n += 1
            if verbose:
                print(f'failed ({n}), waiting...')
            if n >= 10:
                print(f'failed to create {cls.__name__} with attributes {attributes}: {err=}, {type(err)=}')
                break
            time.sleep(3)



def upload_image(item, file):
    n = 0
    while True:
        try:
            rc = item.uploadImage(file)
            return rc
        except Exception as err:
            n += 1
            if verbose:
                print(f'failed ({n}), waiting...')
            if n >= 10:
                print(f'failed to upload image {file}: {err=}, {type(err)=}')
                break
            time.sleep(3)



def upload_attachment(item, file, comment=None):
    n = 0
    while True:
        try:
            rc = item.uploadAttachment(file, comment=comment)
            return rc
        except Exception as err:
            n += 1
            if verbose:
                print(f'failed ({n}), waiting...')
            if n >= 10:
                print(f'failed to upload attachment {file}: {err=}, {type(err)=}')
                break
            time.sleep(3)

def create_it_category_w_parent(category, category_map, parent_id, inventree_api):
    category_id = int(category['@id'].rpartition("/")[2])

    if parent_id:
        parent_it_pk = category_map[parent_id]
    else:
        parent_it_pk = None

    if verbose:
        print(f'create PartCategory "{category["name"]}", parent:{parent_it_pk}')
    icategory = create(PartCategory, inventree_api, {
        'name': category["name"],
        'description': category["description"],
        'parent': parent_it_pk,
        })
    return category_id, icategory

def create_child_categories(parent_category,category_map, inventree_api):
    '''recursively runs through all childs, child-childs, ... and creates them in inventree'''
    for sub_category in parent_category["children"]:
        if not int(sub_category['@id'].rpartition("/")[2]) in category_map:
            # category doesn't yet exist, create it!
            parent_id = int(sub_category['parent'].rpartition("/")[2])
            id, icategory = create_it_category_w_parent(sub_category, category_map, parent_id, inventree_api)
            category_map[id] = icategory.pk
        # continue recursing through children entries
        parent_category = sub_category
        category_map = create_child_categories(parent_category, category_map, inventree_api)
    return category_map

def retry(retries, func, *args, **kwargs):
    for attempt in range(retries):
        try:
            func(*args, **kwargs)
            break
        except Exception as e:
            print(f'Error running {func.__name__} for {retries}. time. Error message:')
            print(f'\t{str(e)}')
            print('trying again...')


def copy_stock_history(pkpr_key, partkeepr_url, partkeepr_auth, inventree_api, stock_item_pk):
    pkpr_filter = '[{"subfilters":[],"property":"part","operator":"=","value":"/partkeepr/api/parts/' + str(pkpr_key) +'"}]'
    pkpr_stock_changes = getFromPartkeepr(f'/api/stock_entries?filter={pkpr_filter}', partkeepr_url, partkeepr_auth)

    if len(pkpr_stock_changes) == 0:
        if verbose:
            print('No Stock changes available for this part. Continue..')
        return

    stock_changes = [entr['stockLevel'] for entr in pkpr_stock_changes]
    
    #generate stock changes without crossing below 0 (every stock change below 0 is no stock change)
    stock_levels = [sum(stock_changes[0:i+1]) for i in range(len(stock_changes))]
    stock_levels_no_negative = [max(0,entr) for entr in stock_levels] # delete negative entries for inventree
    stock_levels_no_negative.insert(0,0)
    stock_changes_no_negative = [0 - (stock_levels_no_negative[i] - el) for i,el in enumerate(stock_levels_no_negative[1:])]

    if verbose:
       print(f"Copying stock history for Part: {pkpr_stock_changes[0]['part']['name']}")
    #upload to Inventree
    part_StockItem = StockItem(api=inventree_api, pk=stock_item_pk)
          
    for i, pkpr_el in enumerate(pkpr_stock_changes):
        time = datetime.datetime.strptime(pkpr_el['dateTime'], '%Y-%m-%dT%H:%M:%S%z').strftime('%d.%m.%Y %H:%M')
        comment = pkpr_el['comment'] if pkpr_el['comment'] != None else '-'
        user = pkpr_el['user']['username'] if pkpr_el['user'] else ''
        note = f"(PartKeepr) {time} {user}: {comment}"

        if stock_changes[i] != stock_changes_no_negative[i]:
            note += ' (Entry adjusted to prevent below 0 stock!)'

        price = None
        if pkpr_el['price']:
            if float(pkpr_el['price']) == 0:
                price = float(pkpr_el['price'])

        stock_update = stock_changes_no_negative[i]
        
        if stock_update > 0:
            retry(10,part_StockItem.addStock, quantity=stock_update, notes = note)
        else:
            retry(10,part_StockItem.removeStock, quantity=-stock_update, notes = note)


def usage():
    print("""partkeepr-to-inventree [options]
    -v     --verbose        operate more verbosely
    -h     --help           show this usage information
    -p URL --partkeepr=URL  use this URL to connect PartKeepr
    -i URL --inventree=URL  use this URL to connect InvenTree
    -w X   --wipe=X         wipe given kind of objects
           --wipe-all       wipe all objects
    --default-currency=STR  use 3 letter currency string as default
    --copy-history          copy stock history into InvenTree Stock Tracking
object kinds are Part, PartCategory, StockLocation, Company
URLs are given as http[s]://USER:PASSWORD@host.do.main""")



def main():

    global verbose

    try:
        opts, args = getopt.getopt(sys.argv[1:], "hvp:i:w:", ["help", "verbose", "partkeepr=", "inventree=", "wipe=", 
                                                              "wipe-all", "default-currency=", "copy-history"])
    except getopt.GetoptError as err:
        usage()
        sys.exit(2)

    partkeepr_auth_url = DEFAULT_PARTKEEPR
    inventree_auth_url = DEFAULT_INVENTREE
    default_currency = DEFAULT_CURRENCY
    wipe = []

    for o, a in opts:
        if o in ("-v", "--verbose"):
            verbose = True
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-p", "--partkeepr"):
            partkeepr_auth_url = a
        elif o in ("-i", "--inventree"):
            inventree_auth_url = a
        elif o in ("-w", "--wipe"):
            wipe.append(a)
        elif o in ("--wipe-all"):
            wipe = ["Part", "PartCategory", "StockLocation", "Company" ]
        elif o in ("--default-currency"):
            default_currency = a
        elif o in ("--copy-history"):
            copy_history = True

    parts1 = inventree_auth_url.partition("//")
    parts2 = parts1[2].rpartition("@")
    parts3 = parts2[0].partition(":")
    url = f'{parts1[0]}//{parts2[2]}'
    inventree = InvenTreeAPI(url, username=parts3[0], password=parts3[2])

    parts1 = partkeepr_auth_url.partition("//")
    parts2 = parts1[2].rpartition("@")
    parts3 = parts2[0].partition(":")
    partkeepr_url = f'{parts1[0]}//{parts2[2]}'
    partkeepr_auth = (parts3[0], parts3[2])

    for table in wipe:
        if table == "Part":
            print(f'deleting StockItems...')
            stock_items = StockItem.list(inventree)
            for stock_item in stock_items:
                if verbose:
                    print(f'delete StockItem "{stock_item.part}"')
                stock_item.delete()
            print(f'deleting Parts...')
            parts = Part.list(inventree)
            for part in parts:
                if verbose:
                    print(f'delete Part "{part.name}"')
                try:
                    part._data['active'] = False
                    part._data['image'] = None      # needs to be removed, as otherwise saving doesn't work
                    part.save()
                    part.delete()
                except Exception as err:
                    print(f'deleting Part "{part.name}" failed')

        elif table == "PartCategory":
            print(f'deleting PartCategories...')
            categories = PartCategory.list(inventree)
            for category in categories:
                if verbose:
                    print(f'delete PartCategory "{category.name}"')
                try:
                    category.delete()
                except Exception as err:
                    print(f'failed')
        elif table == "StockLocation":
            print(f'deleting StockLocations...')
            locations = StockLocation.list(inventree)
            for location in locations:
                if verbose:
                    print(f'delete StockLocation "{location.name}"')
                try:
                    location.delete()
                except Exception as err:
                    print(f'deleting StockLocation "{location.name}" failed')
        elif table == "Company":
            print(f'deleting Companies...')
            companies = Company.list(inventree)
            for company in companies:
                if verbose:
                    print(f'delete Company "{company.name}"')
                try:
                    company.delete()
                except Exception as err:
                    print(f'deleting Company "{company.name} failed')
        else:
            print(f'unknown table {table} to wipe')

    companies = getFromPartkeepr("/api/manufacturers", partkeepr_url, partkeepr_auth)

    print(f'found {len(companies)} manufacturers, creating Companies...')

    company_map = {} # mapped by name
    for company in companies:
        if not company["name"] in company_map:
            if verbose:
                print(f'create Company "{company["name"]}"')
            if ("url" in company) and (company["url"] != None):
                website = company["url"]
                if (not "http" in website) and len(website) >= 3:
                    website = "https://" + website
            else:
                website = ""
            icompany = create(Company, inventree, {
                'name': company["name"],
                'website': website,
                'is_customer': 0,
                'is_manufacturer': 1,
                'is_supplier': 0,
            })            
            company_map[company["name"]] = icompany.pk
            if ("icLogos" in company) and (company["icLogos"] != None) and len(company["icLogos"]) >= 1:
                path = getImageFromPartkeepr(company["icLogos"][0]["@id"], partkeepr_url, partkeepr_auth, filename=company["icLogos"][0]["originalFilename"])
                if verbose:
                    print(f'uploading logo {company["icLogos"][0]["originalFilename"]}')
                upload_image(icompany, path)
                os.unlink(path)

    companies = getFromPartkeepr("/api/distributors", partkeepr_url, partkeepr_auth)

    print(f'found {len(companies)} distributors, creating Companies...')

    for company in companies:
        if not company["name"] in company_map:
            if verbose:
                print(f'create Company "{company["name"]}"')
            if ("url" in company) and (company["url"] != None):
                website = company["url"]
                if not "http" in website:
                    website = "https://" + website
            else:
                website = ""
            icompany = create(Company, inventree, {
                'name': company["name"],
                'website': website,
                'is_customer': 0,
                'is_manufacturer': 0,
                'is_supplier': 1,
            })            
            company_map[company["name"]] = icompany.pk
            if ("icLogos" in company) and (company["icLogos"] != None) and len(company["icLogos"]) >= 1:
                path = getImageFromPartkeepr(company["icLogos"][0]["@id"], partkeepr_url, partkeepr_auth, filename=company["icLogos"][0]["originalFilename"])
                if verbose:
                    print(f'uploading logo {company["icLogos"][0]["originalFilename"]}')
                upload_image(icompany, path)
                os.unlink(path)
        else:
            print(f'Company "{company["name"]} already exists (as manufaturer?) while creating supplier')
            sys.exit(1)

    categories = getFromPartkeepr("/api/part_categories", partkeepr_url, partkeepr_auth)
    
    print(f'found {len(categories)} part categories, creating PartCategories...')

    category_map = {} # mapped by @id
    for category in categories:
        if not int(category['@id'].rpartition("/")[2]) in category_map:
            if category["parent"]:
                parent_id = int(category["parent"]["@id"].rpartition("/")[2])
            else:
                parent_id = None

            id, icategory = create_it_category_w_parent(category, category_map, parent_id, inventree)
            category_map[id] = icategory.pk
            
            parent_category = category
            category_map = create_child_categories(parent_category,category_map, inventree)

    # we convert Partkeepr location categories and locations to an
    # IntenTree hiercarchy of stock locations
    location_categories = getFromPartkeepr("/api/storage_location_categories", partkeepr_url, partkeepr_auth)

    print(f'found {len(location_categories)} location categories, creating StockLocations...')

    location_map = {} # mapped by @id
    for location in location_categories:
        if location["parent"]:
            parent_pk = location_map[location["parent"]["@id"]]
        else:
            parent_pk = None
        if location["description"]:
            description = location["description"]
        else:
            description = "-"
        if verbose:
            print(f'create StockLocation "{location["name"]}", parent:{parent_pk}')
        ilocation = create(StockLocation, inventree, {
            'name': location["name"],
            'description': description,
            'parent': parent_pk,
            })
        location_map[location['@id']] = ilocation.pk

    locations = getFromPartkeepr("/api/storage_locations", partkeepr_url, partkeepr_auth)

    print(f'found {len(locations)} locations, creating StockLocations...')

    for location in locations:
        if location["category"]:
            parent_pk = location_map[location["category"]["@id"]]
        else:
            parent_pk = None
        if verbose:
            print(f'create StockLocation "{location["name"]}", parent:{parent_pk}')
        ilocation = create(StockLocation, inventree, {
            'name': location["name"],
            'description': location["name"], # no description on PartKeepr
            'parent': parent_pk,
            })
        location_map[location['@id']] = ilocation.pk

    parts = getFromPartkeepr("/api/parts", partkeepr_url, partkeepr_auth)

    print(f'found {len(parts)} parts, creating Parts, StockItems, ')

    supplier_part_map = {} 
    created_IPNs_map = {} #key = "IPN"+"name"
    for part in parts:
        #print(part)
        category_pk = category_map[int(part["category"]["@id"].rpartition("/")[2])]
        name = part["name"].strip() #remove leading + trailing space, as it will anyways be done by inventree api
        if part["storageLocation"]:
            try:
                location_pk = location_map[part["storageLocation"]["@id"]]
            except:
                location_pk = None
                print(f'could not handle storageLocation {part["storageLocation"]["@id"]} while creating Part {name}')
        else:
            location_pk = None
        if part["averagePrice"]:
            price = float(part["averagePrice"])
        else:  
            price = None
        revision = None
        if ("description" in part) and (part["description"] != None) and (len(part["description"]) >= 1):
            description = part["description"]
        else:
            description = ""
        if len(description) > 100:
            description = description[:97] + "..."
        if part["internalPartNumber"].strip() == '' or part["internalPartNumber"] == '-':
            ipn = '#' + str(part["@id"].rpartition("/")[2])
        else:
            ipn = part["internalPartNumber"]
        units = None
        if ("partUnit" in part) and (part["partUnit"] != None) and "shortName" in part["partUnit"]:
            units = part["partUnit"]["shortName"]
        quantity = max(0,part["stockLevel"]) # Inventree does not allow stock below 0
        notes = f"**Partkeepr Status:** {part['status']}\n\n" if part["status"] != "" else ""
        notes += part["comment"]
        if (ipn+name) not in created_IPNs_map or name != created_IPNs_map[(ipn+name)]['name']:
            #check entry with same IPN and name were created before
            if verbose:
                print(f'create Part "{part["name"]}", category:{category_pk}, quantity:{quantity}')
            ipart = create(Part, inventree, {
                'name': name,
                'description': description,
                'IPN': ipn,
                'category': category_pk,
                'location': location_pk,
                'active': True,
                'virtual': False,
                'minimum_stock': part["minStockLevel"],
                'notes': notes,
                'revision': revision,
                #'link': xxx,
                #'image': xxx,
                'units': units,
                'assembly': part["metaPart"],
                })
            if ipart != None:
                created_IPNs_map[(ipn+name)] = ipart
        else: # Part Entry already created, only add the StockItem and continue with next Part
            if verbose:
                print(f'create additional StockItem for "{created_IPNs_map[ipn+name]["name"]}", category:{category_pk}, quantity:{quantity}')
            istock = create(StockItem, inventree, {
                'part': created_IPNs_map[(ipn+name)].pk,
                'quantity': 0 if copy_history else quantity,
                'delete_on_deplete': False,
                'location': location_pk,
                'notes': part['partCondition'] if part["partCondition"] != "" else None
            })
            if copy_history:
                copy_stock_history(
                    pkpr_key = int(part["@id"].rpartition("/")[2]),
                    partkeepr_url = partkeepr_url,
                    partkeepr_auth = partkeepr_auth,
                    inventree_api = inventree,
                    stock_item_pk = istock.pk,
                )
            continue
        if verbose:
            print(f'create StockItem "{part["name"]}", category:{category_pk}, quantity:{quantity}')
        istock = create(StockItem, inventree, {
            'part': ipart.pk,
            'quantity': 0 if copy_history else quantity,
            'delete_on_deplete': False,            
            'location': location_pk,
            'notes': part['partCondition'] if part["partCondition"] != "" else None
        })
        if copy_history:
            copy_stock_history(
                pkpr_key = int(part["@id"].rpartition("/")[2]),
                partkeepr_url = partkeepr_url,
                partkeepr_auth = partkeepr_auth,
                inventree_api = inventree,
                stock_item_pk = istock.pk,
            )
        impart = None
        if (part["manufacturers"] != None) and (len(part["manufacturers"]) >= 1):
            for manufacturer in part["manufacturers"]:
                if manufacturer["manufacturer"] == None:
                    mpk = None
                    print(f'no actual manufacturer data known while creating ManufacturerPart {name}')
                elif manufacturer["manufacturer"]["name"] in company_map:
                    mpk = company_map[manufacturer["manufacturer"]["name"]]
                else:
                    mpk = None
                    print(f'manufacturer "{manufacturer["manufacturer"]["name"]}" not known as a Company while creating ManufacturerPart {name}')
                if (manufacturer["partNumber"] != None) and (len(manufacturer["partNumber"]) >= 1):
                    mpn = manufacturer["partNumber"]
                else:
                    mpn = "?" # XXX None
                    print(f'manufacturer part number unknown while creating ManufacturerPart {name}')
                if (mpk != None) and (mpn != None):
                    if verbose:
                        print(f'create ManufacturerPart "{part["name"]}"')
                    impart = create(ManufacturerPart, inventree, {
                        'part': ipart.pk,
                        'manufacturer': mpk,
                        'MPN': mpn,
                        })
        if (part["distributors"] != None) and (len(part["distributors"]) >= 1):
            for distributor in part["distributors"]:
                #assign distributor
                if distributor["distributor"]["name"] in company_map:
                    spk = company_map[distributor["distributor"]["name"]]
                else:
                    spk = None
                    print(f'distributor "{distributor["distributor"]["name"]}" not known as a Company while creating SupplierPart {name}')
                #assign manufacturer
                mpk = None
                if impart != None and len(part["manufacturers"]) == 1: #assignment only clear if only one manufacturer is assigned
                    mpk = impart.pk
                #assign sku
                if len(distributor["sku"]) >= 1:
                    sku = distributor["sku"]
                elif len(distributor["orderNumber"]) >= 1:
                    sku = distributor["orderNumber"]
                else:
                    sku = "?" # must not be an empty string?!
                    if verbose:
                        print(f'distributor SKU not defined while creating SupplierPart "{name}", using a "-" placeholder')
                if (spk != None) and (sku != None):
                    key = f'{ipart.pk}:{spk}:{sku}'
                    if not key in supplier_part_map:
                        if verbose:
                            print(f'create SupplierPart "{part["name"]}"')
                        #print(part)
                        #print(f'XXX {ipart.pk} {spk}({distributor["distributor"]["name"]}) {sku}')
                        ispart = create(SupplierPart, inventree, {
                            'part': ipart.pk,
                            'supplier': spk,
                            'SKU': sku,
                            'manufacturer_part': mpk,
                            })
                        supplier_part_map[key] = ispart
                    else:
                        if verbose:
                            print(f'SupplierPart matching "{key}" for Part "{name}" was already created. Just add additional PriceBreak.')
                    if distributor['price'] != None and distributor['price'] != "0.0000":
                        if distributor['currency'] == None:
                            currency = default_currency
                        else:
                            currency = distributor['currency']
                        I_pr_break = create(SupplierPriceBreak, inventree, {
                            'part': supplier_part_map[key].pk,
                            'quantity': distributor['packagingUnit'],
                            'price': distributor['price'],
                            'supplier': spk,
                            'price_currency': currency
                        })
        if (part["attachments"] != None) and len(part["attachments"]) >= 1:
            for attachment in part["attachments"]:
                filename = re.sub('\?[^ ]{0,}','',attachment["originalFilename"]) # remove ?[...] (sometimes found in pkpr after file type)
                # upload first found image as displayed picture
                if attachment["isImage"] and ipart._data['image'] == None:
                    
                    path = getImageFromPartkeepr(attachment["@id"], partkeepr_url, partkeepr_auth, filename=filename)
                    if path != None: #sometimes the source file might be deleted in partkeepr -> skip these
                        if verbose:
                            print(f'uploading image {path} for Part "{name}"')
                        upload_image(ipart, path)
                        os.unlink(path)
                    else:
                        print(f'Failed to upload file "{filename}" to part "{part["name"]}". Partkeepr did not provide the file!')
                path = getFileFromPartkeepr(attachment["@id"], partkeepr_url, partkeepr_auth, filename=filename)
                if path != None: #sometimes the source file might be deleted in partkeepr -> skip these
                    if verbose:
                        print(f'uploading attachment {path} for Part "{name}"')
                    if (attachment["description"] != None) and (len(attachment["description"]) >= 1):
                        comment = attachment["description"]
                    else:
                        comment = None
                    upload_attachment(ipart, path, comment=comment)
                    os.unlink(path)
                else:
                    print(f'Failed to upload file "{filename}" to part "{part["name"]}". Partkeepr did not provide the file!')



if __name__ == '__main__':
    main()
