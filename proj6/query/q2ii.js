// Task 2ii

db.movies_metadata.aggregate([
    // TODO: Write your query here


    {$project: {_id: 1,
    		    tagline: {$split: ["$tagline", " "]}}},

    {$unwind: "$tagline"},

    {$project: {_id: 1,
    		    tagline: {$trim: {input: "$tagline", chars: ".,?!"}}}},

    {$project: {_id: 1,
        		tagline: {$toLower: "$tagline"}}},

    {$project: {_id: 1,
    		    tagline: 1,
    		    len: {$strLenCP: "$tagline"}}},

    {$match: {len: {$gt: 3}}},

    {$group: {_id: "$tagline",
    		  count: {$sum: 1}}},

    {$sort: {count: -1}},

    {$limit: 20}

]);