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
from inventree.company import Company, ManufacturerPart, SupplierPart



DEFAULT_PARTKEEPR = "https://admin:password@partkeepr.ibr.cs.tu-bs.de"
DEFAULT_INVENTREE = "http://admin:password@inventree.ibr.cs.tu-bs.de:1337"

verbose = False



MY_USERNAME = 'admin'
MY_PASSWORD = 'xxx'
api = "/api/parts"
searchfilter = '?filter={"property":"name","operator":"LIKE","value":"%s%%"}'
auth=('steinb','xxx')



class part(object):

    def __init__(self, req):

        self.name = req["name"]
        self.description = req["description"]
        self.id = req["@id"][req["@id"].rfind("/")+1:]
        try:
            self.footprint = req["footprint"]["name"]
        except TypeError:
            self.footprint = ""
        try:
            self.category =  req["category"]["name"]
        except TypeError:
            self.category = ""
        try:
            self.categoryPath = req["category"]["categoryPath"].split(u' ➤ ')
        except TypeError:
            self.categoryPath =""
        try:
            self.storageLocation = req["storageLocation"]["name"]
        except TypeError:
            self.storageLocation=""
        try:
            self.MFR = req["manufacturers"][0]["manufacturer"]["name"]
        except (TypeError,IndexError):
            self.MFR = "-"
        try:
            self.MPN = req["manufacturers"][0]["partNumber"]
        except (TypeError , IndexError):
            self.MPN = "-"
        try:
            self.IPN = req['internalPartNumber']
        except (TypeError, IndexError):
            self.IPN =self.MPN
        try:
            self.stock = req['stockLevel']
        except (TypeError, IndexError):
            self.stock = 1
        try:
            self.price = req['averagePrice']
        except (TypeError, IndexError):
            self.price = 0

    def getValues(self):
        return [self.category, self.name, self.description, self.footprint]

    def __str__(self):
        return '%s %s %s %s ' % (self.name, self.description, self.footprint, self.category)


"""
def getPartsValues(listparts):
    pvales = []
    for p in listparts:
        pvales.append(p.getValues())
    return pvales



def searchComponent(value):
    url = server+api+searchfilter

    r = requests.get( url % value,auth=auth)

    if (r.status_code == 200):
        listofparts = []
        for p in r.json()["hydra:member"]:
            listofparts.append(part(p))

        return listofparts

    else:
        return None



def getDefaultHdr():
    return ["   Category   ","Value","        Description            ","Footprint"]

def getProjectList():
    url = server + "/api/projects"
    r = requests.get(url, auth=auth)
    if (r.status_code == 200):
        listofprojects = []
        for p in r.json()["hydra:member"]:
            listofprojects.append(p["name"])
        return listofprojects
    else:
        return []

def createProject(projectName,projectDescript,parts):
    projectDict = dict(name=projectName,description=projectDescript)
    #{"quantity":1,"remarks":"R1","overageType":"","overage":0,"lotNumber":"","part":"/api/parts/4"}
    partList = []
    for partid,partv in parts.items():
        partList.append({"quantity": len(partv['remark']), "remarks": "%s" % (",".join(partv['remark'])), "overageType": "", "overage": 0, "lotNumber": "", "part": "/api/parts/%s" % partid})

    projectDict['parts']=partList

    url = server + "/api/projects"
    r = requests.post(url, data=json.dumps(projectDict),auth=auth)
    if r.status_code == 201:
        return True
    return False

def buildPartCategoriesa(pcat):
    #print(pcat)
    #cat = []
    #print(len(pcat))
    #return
    #print(pcat[5]['name'])
    cat = {}
    for p in pcat:
        #print(p['name']+" "+p['@id'])
        #cat[p] = p['children']
        #children = {}
        children=[]
        for c in p['children']:
            children.append(c['name'])
            #print("children ", c['name'])
            #buildPartCategories(c['children'])
        #print(pcat['name'])
        cat[p['name']]=children
    print(cat)
    return cat
    for p in pcat["name"]:
        #print (p)
        l = [p["name"]]
        for c in p["children"]:
            l.append(buildPartCategories(c))
        cat.append(l)
    return cat
"""



def getPartCategories():
    url = server + "/api/part_categories"
    r = requests.get(url, auth=auth)

    if (r.status_code == 200):

        #print(pprint.pprint(r.json()))
        #print(r.json()["hydra:member"])
        listofcategories = buildPartCategories(r.json()["hydra:member"])
        #print(listofcategories)
        print(listofcategories.keys())
        pass
        #return listofprojects
    else:
        return []



"""
def getPartkeeprParts(url, auth):

    full_url = f'{url}/api/parts?itemsPerPage=100000'
    r = requests.get(full_url, auth=auth)

    if (r.status_code == 200):
        return r.json()["hydra:member"]
    return None



def getPartkeeprCategories(url, auth):

    full_url = f'{url}/api/part_categories?itemsPerPage=100000'
    r = requests.get(full_url, auth=auth)

    if (r.status_code == 200):
        return r.json()["hydra:member"]
    return None
"""


def recursiveCat(pathlist,catdict):
    print(pathlist, catdict)
    if (len(pathlist) >= 1):
        node = recursiveCat(pathlist[1:], pathlist[0])
        print (node)
        print (catdict)
        return [catdict,node]
    return catdict



    print(pathlist)
    print(catdict)
    if (len(pathlist)>1):
        if pathlist[0] in catdict:
            twig = catdict[pathlist[0]]['children']
        else:
            twig = {'childrenlist':[],'children':{}}
        leaf = recursiveCat(pathlist[1:],twig)
        print("leaf %s" % leaf)
        print("pathlist %s" % pathlist)
        print("twig %s" % twig)
        twig['childrenlist'].extend(leaf.keys())
        twig['children'].update(leaf)
        print("twig %s" % twig)
        set(twig['childrenlist'])
        print("twig %s" % twig)

        print("catdict %s" % catdict)

        catdict['childrenlist'].extend(twig.keys())
        #print(catdict['childrenlist'])
        #set(catdict['childrenlist'])
        #print(catdict['childrenlist'])
        catdict['children'].update(twig)


    node = {pathlist[0]: {'childrenlist':[],"children":{}}}
    return node

def getPartCategories(allparts):
    #given the list of parts build up a dictionary of dictionaries

    for part in allparts:
        print(part.name)
        partcat = part.category
        print(partcat)
        partcatpath = part.categoryPath
        print(partcatpath)
        partparts = partcatpath.split(u' ➤ ')
        print(part.name)
        print(partparts)
        catdict=[]
        print("recursiveCat")
        cat = recursiveCat(partparts,catdict)
        print (cat)
        print (catdict)
        break




def checkAndCreatePartCat(partnodeCat,parent):
    partnodeCat = partnodeCat.replace('/','-')
    try:
        parent = parent.replace('/', '-')
    except:
        pass
    #print("checkAndCreatePartCat %s %s" % (partnodeCat,parent))
    cats = PartCategory.list(api,search=partnodeCat)
    for c in cats:
        if (c.name==partnodeCat):
            #print ("found ",partnodeCat)
            #print(c.name)
            if (c.parent==None) and (parent==None):
                return c
            if not(c.parent==None):
                parentCat = PartCategory(api,c.parent)
                #print("parentCat ",parentCat)
                #print("parentCat ", parentCat.name)
                if (parentCat.name == parent):
                    return c




    if (parent==None):
        cat = PartCategory.create(api,{
            'name' : partnodeCat,
            'description' : ''
        })
        #print("created ",cat," ",cat.pk)
        return cat
    else:
        parentCat = PartCategory.list(api, search=parent)
        #print("got parent ",parentCat)
        if (len(parentCat)>0):
            parentpk=None
            for pc in parentCat:
                if (pc.name==parent):
                    parentpk = pc
                    break

            #print(parentpk.name)
            #print(parentpk.pk)
            cat = PartCategory.create(api, {
                'name': partnodeCat,
                'description': '',
                'parent' : parentpk.pk
            })
            print("created ",cat," ",cat.pk)
            return cat
        else:
            print("checkAndCreatePartCat %s %s" % (partnodeCat, parent))
            print("Error parent given but not created previously")
            return None

def getorCreateLocation(part):
    print(part.name, " loca ",part.storageLocation)
    if (len(part.storageLocation)>0):
        itloca = StockLocation.list(api,search=part.storageLocation)
        for loc in itloca:
            if (loc.name == part.storageLocation):
                return loc
        return 0

    else:
        #create or return unknownloadtion
        itloca = StockLocation.list(api, search='UNKNOWN')
        if (len(itloc)>0):
            return itloca[0]
        return 0

def createITPart(part,ITCat):
    print("create part %s cat %s" % (part,ITCat.name))
    if len(part.description)==0:
        part.description=part.name
    np = Part.create(api, {
        'name' : part.name,
        'description' : part.description,
        'category' : ITCat.pk,
        'active' : True,
        'virtual' : False,
        'IPN' : part.IPN
    })
    return np

def bubu():
    allPKparts=partkeepr.getallParts()
    for pkpart in allPKparts:
        catpath = pkpart.categoryPath[1:]
        root = None
        for p in catpath:

            catforPart = checkAndCreatePartCat(p,root)
            root = p
        newPart = createITPart(pkpart, catforPart)
        print(newPart._data)
        itloc = getorCreateLocation(pkpart)
        if itloc == 0:
            itloc = None
        print("XXX5 %s, %s, %s" % (newPart.pk, pkpart.stock, itloc))
        stockit = StockItem.create(api,{
            'part' : newPart.pk,
            'quantity' : pkpart.stock,
            'location' : itloc,
            #'location' : itloc.pk
        })
        print("XXX6")



def getFromPartkeepr(url, base, auth):

    full_url = f'{base}{url}?itemsPerPage=100000'
    r = requests.get(full_url, auth=auth)

    if (r.status_code == 200):
        return r.json()["hydra:member"]
    return None



def getImageFromPartkeepr(url, base, auth, filename="image"):

    full_url = f'{base}{url}/getImage'
    r = requests.get(full_url, auth=auth, stream=True)

    if (r.status_code == 200):
        r.raw.decode_content = True
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix="f-%s" % (filename))
        shutil.copyfileobj(r.raw, tmp)
        return tmp.name
    return None



def getFileFromPartkeepr(url, base, auth, filename="file"):

    full_url = f'{base}{url}/getFile'
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



def usage():
    print("""partkeepr-to-inventree [options]""")



def main():

    global verbose

    try:
        opts, args = getopt.getopt(sys.argv[1:], "hvp:i:w:", ["help", "verbose", "partkeepr=", "inventree=", "wipe=", "wipe-all"])
    except getopt.GetoptError as err:
        usage()
        sys.exit(2)

    partkeepr_auth_url = DEFAULT_PARTKEEPR
    inventree_auth_url = DEFAULT_INVENTREE
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
        id = int(category['@id'].rpartition("/")[2])
        if category["parent"]:
            parent_id = int(category["parent"]["@id"].rpartition("/")[2])
            parent_pk = category_map[parent_id]
        else:
            parent_pk = None
        if verbose:
            print(f'create PartCategory "{category["name"]}", parent:{parent_pk}')
        icategory = create(PartCategory, inventree, {
            'name': category["name"],
            'description': category["description"],
            'parent': parent_pk,
            })
        category_map[id] = icategory.pk

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
    for part in parts:
        #print(part)
        category_pk = category_map[int(part["category"]["@id"].rpartition("/")[2])]
        name = part["name"]
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
        # there might duplicates purely by name, hence we need revisions to differentiate
        revision = None
        if ("description" in part) and (part["description"] != None) and (len(part["description"]) >= 1):
            description = part["description"]
        else:
            description = ""
        if len(description) > 100:
            description = description[:97] + "..."
        if ("footprint" in part) and (part["footprint"] != None) and "name" in part["footprint"]:
            revision = part["footprint"]["name"]
        if len(description) >= 1:
            if revision:
                revision = f'{revision}, {description}'
            else:
                revision = description
        if revision and (len(revision) > 100):
            revision = revision[:97] + "..."
        units = None
        if ("partUnit" in part) and (part["partUnit"] != None) and "shortName" in part["partUnit"]:
            units = part["partUnit"]["shortName"]
        if verbose:
            print(f'create Part "{part["name"]}", category:{category_pk}, quantity:{part["stockLevel"]}')
        ipart = create(Part, inventree, {
            'name': name,
            'description': description,
            'IPN': part["internalPartNumber"],
            'category': category_pk,
            'location': location_pk,
            'active': True,
            'virtual': False,
            'minimum_stock': part["minStockLevel"],
            'comment': part["comment"],
            'revision': revision,
            #'link': xxx,
            #'image': xxx,
            'units': units,
            'assembly': part["metaPart"],
            })
        if verbose:
            print(f'create StockItem "{part["name"]}", category:{category_pk}, quantity:{part["stockLevel"]}')
        istock = create(StockItem, inventree, {
            'part': ipart.pk,
            'quantity': part["stockLevel"],
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
                            #'link': # ??? orderNumber ???
                            })
                        supplier_part_map[key] = ispart
                    else:
                        print(f'there is already a SupplierPart matching "{key}" for Part "{name}"')
        if (part["attachments"] != None) and len(part["attachments"]) >= 1:
            for attachment in part["attachments"]:
                if attachment["isImage"]:
                    # example: SC16IS750IPW,128
                    path = getImageFromPartkeepr(attachment["@id"], partkeepr_url, partkeepr_auth, filename=attachment["originalFilename"])
                    if verbose:
                        print(f'uploading image {path} for Part "{name}"')
                    upload_image(ipart, path)
                    os.unlink(path)
                else:
                    path = getFileFromPartkeepr(attachment["@id"], partkeepr_url, partkeepr_auth, filename=attachment["originalFilename"])
                    if verbose:
                        print(f'uploading attachment {path} for Part "{name}"')
                    if (attachment["description"] != None) and (len(attachment["description"]) >= 1):
                        comment = attachment["description"]
                    else:
                        comment = None
                    upload_attachment(ipart, path, comment=comment)
                    os.unlink(path)



if __name__ == '__main__':
    main()
