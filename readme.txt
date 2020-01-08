Stefou Theodoros - Στέφου Θεόδωρος
AM: cs2190002
email: cs2190002@di.uoa.gr / theo.stefou@gmail.com

======   SCHEMA     ======
The application uses mongoDB version 4.2.2
Database name: nosql

The database has 2 collections:
-logs
-admins

When a field is optional, it does not appear in the document
if it does not exist.

In the repository you can find the python3 scripts I used
to populate the database. The csv files that were used were
the ones I parsed from the previous project's files which had the
same data.

For the generation of the admins collection, I used the Faker module in python and
added some functions to make sure that no duplicate usernames were generated.
For the upvotes, first I calculated the votes with unique integers ranged in
[0,numlogs-1] and then mapped these integers to the database's ids by loading
all the ids and using the map generator in python.
I created 5000 admins, having 370-1000 votes each.
Out of 5.529.502 logs, I generated 2.543.978 unique votes, which is around 46%. That
is greater than the 33.33% that is required.


LOGS

example for access:
{
	"_id": ObjectId("...."),
	"type": "access",
	"timestamp": ISODate("......"),
	"http_method": "GET",
	"source_ip": "109.169.248.247",
	"user_agent_string": "Mozilla/5.0",
	"size": 100,
	"response_status": 200,
	"referer": "http://example.com", //OPTIONAL
	"user_id": "admin15" //OPTIONAL
}

example for dataxceiver:
{
	"_id": ObjectId("...."),
	"type": "receiving/received/served",
	"timestamp": ISODate("......"),
	"source_ip": "10.100.34.153",
	"size": 150, //OPTIONAL,
	"block_ids": ["blk_23423235"], //Exactly one block id in the array
	"dest_ips": ["exactly this ip"] //Exactly one destination ip in the array
}

example for HDFS namesystem:
{
	"_id": ObjectId("...."),
	"type": "replicate/delete",
	"timestamp": ISODate("......"),
	"source_ip": "109.169.248.247",
	"block_ids": ["blk_1", "blk_2", ...],
	"dest_ips": ["ip_1", "ip_2", ...], //OPTIONAL
}

As you can see, even though there is only one block_id and one dest_ip in
dataxceiver blocks, I still created an array of one item and the field name
is in plural so that it is the same as HDFS namesystem. This is because I wanted
to query all block_id and dest_ip related logs the same way. It does not have to be
an array though, because in recent versions of mongoDB, the aggregation stage
$unwind treats single objects as single element arrays. Still, it keeps a "uniform"
schema across the database, so that is why I kept it.

ADMINS

example for admin:
{
	"_id": ObjectId("...."),
	"username": "adminusername",
	"email": "admin@nosql311.com",
	"telephone": "100-100-100-100",
	"upvotes": [ObjectId("...logid_1..."), ObjectId("...logid_2..."), ...]

}

======REST API USAGE======

For the development of the application I used the Flask framework of python.
The application connects to the database using python's pymongo module.
For the testing of the application I used Postman https://www.getpostman.com/

How to use the Rest API of NoSQL-311CI:

All outputs are in JSON format.

In case of an error, the http response has the code 400 BAD REQUEST
and returns a JSON like the following:
{
	"error": str
}

For the following queries 1-11, if successfully executed, the
application returns an http response code 200 OK.

------QUERY 1------
endpoint: 127.0.0.1:5000/nosql/query1
[Input: POST]
{
	"from": "%d-%m-%Y %H:%M:%S",
	"to": "%d-%m-%Y %H:%M:%S"
}

[Output]
[
	{
		"type": string,
		"count": int
	},
	...
]

------QUERY 2------
endpoint: 127.0.0.1:5000/nosql/query2
[Input: POST]
{
	"from": "%d-%m-%Y %H:%M:%S",
	"to": "%d-%m-%Y %H:%M:%S",
	"type": string
}

[Output]
[
	{
		"day": "%d-%m-%Y",
		"count": int
	},
	...
]


------QUERY 3------
endpoint: 127.0.0.1:5000/nosql/query3
[Input: POST]
{
	"day": "%d-%m-%Y"
}

[Output]
[
	{
		"source_ip": string,
		"types": [
			{
				"count": int,
				"type": string
			},
			...
			(up to three log types)
		]
	},
	...
]

------QUERY 4------
endpoint: 127.0.0.1:5000/nosql/query4
[Input: POST]
{
	"from": "%d-%m-%Y %H:%M:%S",
	"to": "%d-%m-%Y %H:%M:%S"
}

[Output]
[
	{
		"http_method": string
	},
	{
		"http_method": string
	}
]

------QUERY 5------
endpoint: 127.0.0.1:5000/nosql/query5
[Input: POST, GET]
-

[Output]
[
	{
		"referer": string
	},
	...
]

------QUERY 6------
endpoint: 127.0.0.1:5000/nosql/query6
[Input: POST, GET]
-

[Output]
[
	{
		"block": string
	},
	...
]

------QUERY 7------
endpoint: 127.0.0.1:5000/nosql/query7
[Input: POST]
{
	"day": "%d-%m-%Y"
}

[Output]
[
	{
		"logid": str,
		"votes": int
	},
	...
]

------QUERY 8------
endpoint: 127.0.0.1:5000/nosql/query8
[Input: POST, GET]
-

[Output]
[
	{
		"username": string,
		"numupvotes": int
	},
	...
]

------QUERY 9------
endpoint: 127.0.0.1:5000/nosql/query9
[Input: POST, GET]
-

[Output]
[
	{
		"username": string,
		"source_ips": int
	},
	...
]

------QUERY 10-----
endpoint: 127.0.0.1:5000/nosql/query10
[Input: POST, GET]
-

[Output]
[
	{
		"logid": string
	},
	...
]

------QUERY 11-----
endpoint: 127.0.0.1:5000/nosql/query11
[Input: POST]
{
	"username": string
}

[Output]
[
	{
		"blocks": [string, string, ...],
		"username": string
	}
]



The following query returns an http response code 201 CREATED,
otherwise 400 BAD REQUEST.

------NEW LOG------
endpoint: 127.0.0.1:5000/nosql/newlog
[Input: POST]
{
	"log": {
		"type": str,
		...log fields...
	}
}

[Output]
A JSON with the log as it was inserted in the database. (_id appended)

------NEW UPVOTE---
endpoint: 127.0.0.1:5000/nosql/newupvote
[Input: POST]
{
	"admin_id": str,
	"log_id": str
}

201 CREATED if successful, otherwise 409 CONFLICT if the vote already exists
[Output]
{
	"updated": boolean
}

------EXTRA--------
I have created the following extra endpoint.

endpoint: 127.0.0.1:5000/nosql/admin/<username>
[Input: POST,GET]
The username string provided in the URL.

[Output]
{
	...The whole admin document that corresponds to the username...
}


======INDEXES======
[admins.upvotes]
There is an index on the upvotes array of admins to achieve fast joins with the logs collection. This is only helpful when joining FROM logs TO admins

-Specifically, this index greatly improves the performance of:

Query 7(input=12-11-2017):
without index 	-> 1m1s
with index 		-> 2.46s

By default, mongoDB indexes the _id of each collection.