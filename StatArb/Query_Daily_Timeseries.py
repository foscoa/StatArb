import pymongo

# Step 1 - Install and import pymongo
import pymongo

# Step 2 - create a mongodb client, use default local host
try:
    client = pymongo.MongoClient()
except Exception:
    print("Error: " + Exception)

# Step 3 - List database names


