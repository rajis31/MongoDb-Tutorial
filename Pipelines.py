from pymongo import MongoClient
import re

# Requires the PyMongo package.
# https://api.mongodb.com/python/current


"""
  # Example of user data document
    {
  "registered": {
    "$date": "2015-02-11T04:22:39.000Z"
  },
  "age": 20,
  "eyeColor": "green",
  "company": {
    "location": {
      "address": "694 Hewes Street",
      "country": "USA"
    },
    "title": "YURTURE",
    "email": "aureliagonzales@yurture.com",
    "phone": "+1 (940) 501-3963"
  },
  "index": 0,
  "name": "Aurelia Gonzales",
  "isActive": false,
  "tags": [
    "enim",
    "id",
    "velit",
    "ad",
    "consequat"
  ],
  "_id": {
    "$oid": "669fea099d76794392826273"
  },
  "gender": "female",
  "favoriteFruit": "banana"
}
 """

# Get the # of active clients
# It first filters for active users and then counts them and returns
# {
#     "activeUsers": 516
# }

client = MongoClient('')
result = client['aggree']['users'].aggregate([
    {
        '$match': {
            'isActive': True
        }
    }, {
        '$count': 'activeUsers'  # Count name
    }
])

# Get the avg age by gender
result = client['aggree']['users'].aggregate([
    {
        '$group': {
            '_id': '$gender',  # You can use "null" or null to get an overall avg
            'averageAge': {
                '$avg': '$age'
            }
        }
    }
])

# Get the top 5 most common fruit
result = client['aggree']['users'].aggregate([
    {
        '$group': {
            '_id': '$favoriteFruit',
            'count': {
                '$sum': 1  # Total up 1's
            }
        }
    }, {
        '$sort': {
            'count': -1  # -1 is ascending and 1 is descending
        }
    }, {
        '$limit': 5  # limit to 5 rows
    }
])

# Get the avg age
result = client['aggree']['users'].aggregate([
    {
        '$group': {
            '_id': None,
            'averageAge': {
                '$avg': '$age'
            }
        }
    }
])


# Group by fruit and get count
result = client['aggree']['users'].aggregate([
    {
        '$group': {
            '_id': '$favoriteFruit',
            'count': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            'count': -1
        }
    }, {
        '$limit': 2
    }
])


# Count by gender
# Counting is done through $sum usually
result = client['aggree']['users'].aggregate([
    {
        '$group': {
            '_id': '$gender',
            'count': {
                '$sum': 1
            }
        }
    }
])

# Which country has the hightest # of users
result = client['aggree']['users'].aggregate([
    {
        '$group': {
            '_id': '$company.location.country', 
            'userCount': {
                '$sum': 1
            }
        }
    }, 
    {
        '$sort': {
            'userCount': -1
        }
    }, 
    {
        '$limit': 2
    }
])

# what is the average # of tags per user 
"""
  - $unwind will basically split apart a document into subdocuments based on the field.
  - If the field is an array, then a new document will be created for each value in the array
"""
result = client['aggree']['users'].aggregate([
    {
        '$unwind': {   
            'path': '$tags'
        }
    }, {
        '$group': {
            '_id': '$_id', 
            'numberOfTags': {
                '$sum': 1
            }
        }
    }, {
        '$group': {
            '_id': None, 
            'avgNumberOfTags': {
                '$avg': '$numberOfTags'
            }
        }
    }
])

# OR
result = client['aggree']['users'].aggregate([
    {
        '$addFields': {
            'numberOfTags': {
                '$size': {
                    '$ifNull': [  # handles cases if document does not have a tags field so it replaces the Null with a []
                        '$tags', []
                    ]
                }
            }
        }
    }, {
        '$group': {
            '_id': None, 
            'averageNumOfTags': {
                '$avg': '$numberOfTags'
            }
        }
    }
])

# How many users have "enim" as one of their tags
result = client['aggree']['users'].aggregate([
    {
        '$match': {
            'tags': 'enim' # Match any document that has enim in the array tags
        }
    }, 
    {
        '$count': 'enim'  # Count up the docs in the first pipeline
    }
])

# What are the names and age who are inactive and have 'velit' as a tag?

result = client['aggree']['users'].aggregate([
    {
        '$match': {
            'isActive': False, 
            'tags': 'velit'
        }
    }, {
        '$project': {
            'age': 1, 
            'name': 1
        }
    }
])

# how many users have phone # starting with '+1 (940)'?
result = client['aggree']['users'].aggregate([
    {
        '$match': {
            'company.phone': re.compile(r"^\\+1 \(940\)")
        }
    }, {
        '$count': 'usersWithSpecialPhoneNumber'
    }
])

# Who has registeed most recently?
result = client['aggree']['users'].aggregate([
    {
        '$sort': {
            'registered': -1
        }
    }, {
        '$limit': 4
    }, {
        '$project': {
            'name': 1, 
            'registered': 1, 
            'favoriteFruit': 1
        }
    }
])

# Categorize users by their favorite fruits 
result = client['aggree']['users'].aggregate([
    {
        '$group': {
            '_id': '$favoriteFruit', 
            'users': {
                '$push': '$name' # Push will append the names of users as an array grouped by favoriteFruit
            }
        }
    }
])

# How many users have 'ad' as the second tag in their list of tags ?
result = client['aggree']['users'].aggregate([
    {
        '$match': {
            'tags.1': 'ad' # tags.1 will refer to the second tag in array
        }
    }, {
        '$count': 'secondTagAd'
    }
])

# Find users who have both enim and id as their tags 
result = client['aggree']['users'].aggregate([
    {
        '$match': {
            'tags': {
                '$all': [
                    'enim', 'id'
                ]
            }
        }
    }
])

# List all companies located in the USA with their corresponding user count
result = client['aggree']['users'].aggregate([
    {
        '$match': {
            'company.location.country': 'USA'
        }
    }, {
        '$group': {
            '_id': '$company.title', 
            'userCount': {
                '$sum': 1
            }
        }
    }
])

# Lookup
result = client['aggree']['books'].aggregate([
    {
        '$lookup': {
            'from': 'authors', 
            'localField': 'author_id', 
            'foreignField': '_id', 
            'as': 'author_details'
        }
    }, 
    {
        '$addFields': {
            'author_details': {
                '$first': '$author_details'
            }
        }
    }
])

# OR
# You can use $arrayElemAt
result = client['aggree']['books'].aggregate([
    {
        '$lookup': {
            'from': 'authors', 
            'localField': 'author_id', 
            'foreignField': '_id', 
            'as': 'author_details'
        }
    }, {
        '$addFields': {
            'author_details': {
                '$arrayElemAt': [
                    '$author_details', 0
                ]
            }
        }
    }
])





