// Task 3ii

db.credits.aggregate([
    // TODO: Write your query here
    {$match: {crew: {$elemMatch: {$and: [{$and: [{id: {$gte:5655}}, {id: {$lte:5655}}]}, {department: "Directing"}]}}}},

    {$unwind: "$crew"},

    {$project: {_id: "$movieId",
                department: "$crew.department",
                ID: "$crew.id"}},

    {$match: {$and: [{$and: [{ID: {$gte:5655}}, {ID: {$lte:5655}}]}, {department: "Directing"}]}},

    {$lookup: {from: "credits",
    		   localField: "_id",
    		   foreignField: "movieId",
    		   as: "lookup"}},

    {$unwind: "$lookup"},
    {$unwind: "$lookup.cast"},

    {$group: {_id: {key1: "$lookup.cast.name", key2: "$lookup.cast.id"},
    	      count: {$sum: 1}}},

    {$project: {_id: 0,
    	        count: 1,
    	        name: "$_id.key1",
    	        id: "$_id.key2"}},

    {$sort: {count: -1, id: 1}},
    {$limit: 5}

]);