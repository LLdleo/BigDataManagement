conn = new Mongo();
db = conn.getDB("local");
db.createCollection('test')

1)
db.test.insert(
    {
        "_id": 20,
        "name": {
            "first": "Alex",
            "last": "Chen"
        },
        "birth": ISODate("1933-08-27T04:00:00Z"),
        "death": ISODate("1984-11-07T04:00:00Z"),
        "contribs": [
            "C++",
            "Simula"
        ],
        "awards": [
            {
                "award": "WPI Award",
                "year": 1977,
                "by": "WPI"
            }
        ]
    }
)

db.test.insert(
    {
        "_id": 30,
        "name": {
            "first": "David",
            "last": "Mark"
        },
        "birth": ISODate(
            "1911-04-12T04:00:00Z"
        ),
        "death": ISODate(
            "2000-11-07T04:00:00Z"
        ),
        "contribs": [
            "C++",
            "FP",
            "Lisp"
        ],
        "awards": [
            {
                "award": "WPI Award",
                "year": 1963,
                "by": "WPI"
            },
            {
                "award": "Turing Award",
                "year": 1966,
                "by": "ACM"
            }
        ]
    }
)

2)
db.test.find(
    {
        $or:[
            {$where:"this.awards.length<3"},
            {contribs:"FP"}
            ]
    })
    
db.test.find(
    {
        $or:[
            {awards:{$size:0}},
            {awards:{$size:1}},
            {awards:{$size:2}},
            {contribs:"FP"}
            ]
    });

3)
db.test.update(
	{
        name: {
            "first": "Guido",
            "last": "van Rossum"
        }
	},
   	{
		$push: { contribs: "OOP" }
   }
);

4)
db.test.update(
	{
        name: {
            "first": "Alex",
            "last": "Chen"
        }
	},
   	{
		$set: {
			comments: [
				"He taught in 3 universities",
				"died from cancer",
				"lived in CA"
			]
		}
   }
);

5)
var contributions=[];
result = db.test.find(
	{
        name: {
            "first": "Alex",
            "last": "Chen"
        }
    }
).forEach(
	function(u){
		contributions=u.contribs
	}
	);
cursor = db.test.aggregate(
        [
        	{$unwind:"$contribs"},
		{$match:{'contribs':{$in: contributions}}},
		{$group:{_id: "$contribs",people:{$push:"$name"}}}
            ]
);
   
