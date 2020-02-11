conn = new Mongo();
db = conn.getDB("local");

db.createCollection("test");

// Question 1
// 1)
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

// 2)
db.test.find(
{
    $or:
    [
        {$where:"this.awards.length<3"},
        {contribs:"FP"}
    ]
})
  
// 3)
db.test.update(
	{
        name: 
        {
            "first": "Guido",
            "last": "van Rossum"
        }
	},
   	{$push: { contribs: "OOP" }}
);

// 4)
db.test.update(
	{
        name: {
            "first": "Alex",
            "last": "Chen"
        }
	},
   	{
		$set: {
            comments: 
            [
				"He taught in 3 universities",
				"died from cancer",
				"lived in CA"
			]
		}
   }
);

// 5)
var contribs=[];
result = db.test.find(
	{
        name: {
            "first": "Alex",
            "last": "Chen"
        }
    }
).forEach(
	function(f){
		contribs=f.contribs
	}
	);
output1 = db.test.aggregate([
    {$unwind:"$contribs"},
	{$match:{'contribs':{$in: contribs}}},
	{$group:{_id: "$contribs",people:{$push:"$name"}}}
]);
output1.forEach(printjson);

// 6)
output2 = db.test.distinct(
	'awards.by'
)
output2.forEach(printjson);


// 7)
db.test.update(
	{"awards.year": 2011},
    {
		$pull: {
			awards: {
				year: 2011
			}
		}
    },
    {multi: true}
);

// 8)
output3 = db.test.aggregate([
	{$unwind:"$awards"},
	{$match:{'awards.year':2001}},
	{$group:{_id: "$name",count:{$sum:1}}},
	{$match:{count:{$gte: 2}}},
    {$project:{name:1}}
]);
output3.forEach(printjson);

// 9)
fetch = db.test.find().sort({ _id: -1 }).limit(1);
max_id = fetch.next()._id
docs = db.test.findOne(
	{
		_id: max_id
    }
);
print(docs);

// 10)
output4 = db.test.findOne(
	{'awards.by': "ACM"}	
);
output4.forEach(printjson);


// Question2
// 1)
db.test.aggregate(
    {$match:{awards:{$exists:true}}},
    {$group:{_id:"$award", count:{$sum:1}}
})

// 2)
db.test.aggregate(
{$match:{birth:{$exists:true}}},
{$group:{_id:{$year:"$birth"},
         idarray:{$addToSet:"$_id"}}
})

// 3)
db.test.find().sort({_id:1}).limit(1);
db.test.find().sort({_id:-1}).limit(1);


// Question 3
parent_categories = [
    { _id: "MongoDB", parent: "Databases"},
    { _id: "dbm", parent: "Databases"},
    { _id: "Databases", parent: "Programming"},
    { _id: "Languages", parent: "Programming"},
    { _id: "Programming", parent: "Books"},
    { _id: "Books", parent: null },
];
db.parent_categories.insert(parent_categories);

child_categories = [
    { _id: "Books", children: ["Programming"]},
    { _id: "Programming", children: ["Languages", "Databases"]},
    { _id: "Languages", children: []},
    { _id: "Databases", children: ["dbm", "MongoDB"]},
    { _id: "dbm", children: []},
    { _id: "MongoDB", children: []},
];
db.child_categories.insert(child_categories);

// 1)
var data1 = [];
var ancestors = [];
var level = 0;
var category1 = db.parent_categories.findOne({_id: "MongoDB"});
data1.push(category1);
while (data1.length > 0) {
	level++;
	var current = data1.pop();
	var parent = db.parent_categories.findOne({_id: current.parent});
	if (parent) {
		ancestors.push({ Name: parent._id, Level: level });
		data1.push(parent);
	}
}
print(tojson(ancestors));

// 2)
var data2 = [];
var visitedIds = {};
var level = 0;
var category2 = db.parent_categories.findOne({_id: "Books"});
data2.push(category2);
while (data2.length > 0) {
	var current = data2.pop();
	var children = db.parent_categories.find({parent: current._id});

	if (!(current.parent in visitedIds)) {
		level++;
		visitedIds[current.parent] = 1;
	}

	while (children.hasNext()) {
		var child = children.next();
		data2.push(child);
	}
}
print("levels: "+level);

// 3)
parent = db.child_categories.findOne(
	{ children: { $in: ["dbm"] }}
);
print(tojson(parent));

// 4)
var data3 = [],
	descendants = [];
var category3 = db.child_categories.findOne({_id: "Books"});
data3.push(category3);
while (data3.length > 0) {
	var current = data3.pop();
	var children = db.child_categories.find({_id: {$in: current.children}});

	while (children.hasNext()) {
		var child = children.next();
		descendants.push(child._id);
		data3.push(child);
	}
}
print(tojson(descendants));
