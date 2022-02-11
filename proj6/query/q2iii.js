// Task 2iii

db.movies_metadata.aggregate([
    // TODO: Write your query here

    {$project: {_id: 1,
    		    budget: {$cond: [{$and: [{$ne: ["$budget", null]},
    								    {$ne: ["$budget", false]},
    								    {$ne: ["$budget", ""]},
    								    {$ne: ["$budget", undefined]}]},  "$budget", "unknown"]}}},
    {$project: {_id: 1,
    		    budget: {$cond: [{$ne: [{$isNumber: "$budget"}, true]}, {$trim: {input: "$budget", chars: " USD$"}}, "$budget"]}}},

    {$project: {_id: 1,
        		budget: {$cond: [{$ne: ["$budget", "unknown"]}, {$toInt: "$budget"}, "$budget"]}}},

    {$project: {_id: 1,
        	    budget: {$cond: [{$isNumber: "$budget"}, {$round: ["$budget", -7]}, "$budget"]}}},

    {$group: {_id: "$budget",
        	  count: {$sum: 1}}},

    {$sort: {_id: 1}},

    {$project: {_id: 0,
        		budget: "$_id",
        		count: 1}}

]);