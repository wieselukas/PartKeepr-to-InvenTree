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

from inventree.api import InvenTreeAPI
from inventree.part import PartCategory, Part
from inventree.stock import StockItem, StockLocation
from inventree.company import Company, ManufacturerPart, SupplierPart, SupplierPriceBreak



DEFAULT_PARTKEEPR = "https://admin:password@partkeepr.ibr.cs.tu-bs.de"
DEFAULT_INVENTREE = "http://admin:password@inventree.ibr.cs.tu-bs.de:1337"
DEFAULT_CURRENCY = "EUR"

verbose = False



def getFromPartkeepr(url, base, auth):

    full_url = f'{base}{url}?itemsPerPage=100000'
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

def usage():
    print("""partkeepr-to-inventree [options]
    -v     --verbose        operate more verbosely
    -h     --help           show this usage information
    -p URL --partkeepr=URL  use this URL to connect PartKeepr
    -i URL --inventree=URL  use this URL to connect InvenTree
    -w X   --wipe=X         wipe given kind of objects
           --wipe-all       wipe all objects
    --default-currency=STR  use 3 letter currency string as default
object kinds are Part, PartCategory, StockLocation, Company
URLs are given as http[s]://USER:PASSWORD@host.do.main""")



def main():

    global verbose

    try:
        opts, args = getopt.getopt(sys.argv[1:], "hvp:i:w:", ["help", "verbose", "partkeepr=", "inventree=", "wipe=", "wipe-all", "default-currency="])
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
                    part._data['image'] = None
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
                'notes': part["comment"],
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
                'quantity': quantity,
                'averagePrice': price,
                'location': location_pk,
            })
            continue
        if verbose:
            print(f'create StockItem "{part["name"]}", category:{category_pk}, quantity:{quantity}')
        istock = create(StockItem, inventree, {
            'part': ipart.pk,
            'quantity': quantity,
            'averagePrice': price,
            'location': location_pk,
        })
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
                if distributor["distributor"]["name"] in company_map:
                    spk = company_map[distributor["distributor"]["name"]]
                else:
                    spk = None
                    print(f'distributor "{distributor["distributor"]["name"]}" not known as a Company while creating SupplierPart {name}')
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
                            'currency': currency
                        })
        if (part["attachments"] != None) and len(part["attachments"]) >= 1:
            for attachment in part["attachments"]:
                if attachment["isImage"]:
                    # example: SC16IS750IPW,128
                    path = getImageFromPartkeepr(attachment["@id"], partkeepr_url, partkeepr_auth, filename=attachment["originalFilename"])
                    if path != None: #sometimes the source file might be deleted in partkeepr -> skip these
                        if verbose:
                            print(f'uploading image {path} for Part "{name}"')
                        upload_image(ipart, path)
                        os.unlink(path)
                    else:
                        print(f'Failed to upload file "{attachment["originalFilename"]}" to part "{part["name"]}". Partkeepr did not provide the file!')
                else:
                    path = getFileFromPartkeepr(attachment["@id"], partkeepr_url, partkeepr_auth, filename=attachment["originalFilename"])
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
                        print(f'Failed to upload file "{attachment["originalFilename"]}" to part "{part["name"]}". Partkeepr did not provide the file!')



if __name__ == '__main__':
    main()
