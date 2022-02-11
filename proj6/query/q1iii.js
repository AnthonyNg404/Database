// Task 1iii

db.ratings.aggregate([
    // TODO: Write your query here

    {$group: {_id: "$rating", count: {$sum: 1}}},

    {$sort: {_id: -1}},

    {$project: {_id: 0, count: 1, rating: "$_id"}}

]);