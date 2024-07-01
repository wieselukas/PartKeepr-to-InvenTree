# PartKeepr to InvenTree

**Forked from the original implementation done by @frsteinb (https://gitlab.ibr.cs.tu-bs.de/steinb/partkeepr-to-inventree)**

A simple tool to convert data from PartKeepr to InvenTree.
It handles many, though not all data structures.

## Usage:

./partkeepr-to-inventree.py -p https://USER:PASSWORD@partkeepr.example.com -i http://admin:PASSWORD@inventree.example.com --wipe-all --default-currency=EUR --copy-history

Add -v for a more verbose operation.

## Contribution

Given that I will hopefully not need to migrate from partkeepr to inventree again, I will unfortunately also not put any more work into the script. But in case you want to fork the project and have questions about the implementation please feel very free to reach out!
