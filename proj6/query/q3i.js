// Task 3i

db.credits.aggregate([
    // TODO: Write your query here
    {$match: {cast: {$elemMatch: {$and: [{id: {$gte:7624}}, {id: {$lte:7624}}]}}}},

    {$unwind: "$cast"},

    {$project: {_id: "$movieId",
                character: "$cast.character",
                ID: "$cast.id"}},

    {$match: {$and: [{ID: {$gte:7624}}, {ID: {$lte:7624}}]}},

    {$lookup: {from: "movies_metadata",
               localField: "_id",
               foreignField: "movieId",
               as: "lookup"}},

    {$project: {_id: 0,
                title: {$first:"$lookup.title"},
                release_date: {$first:"$lookup.release_date"},
    		    character: "$character"}},

    {$sort: {release_date: -1}}
]);
