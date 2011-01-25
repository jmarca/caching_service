var fs = require('fs');

function makeme (dir,cb){
    return function(){
        console.log('makeme: ',dir);
        fs.mkdir(dir,0777,function(err){
            if(err) { console.log(err) ;}
            if(cb) cb();
        });
    };
};

function writeGeoJSON(options,cb){
    return function(){
        fs.writeFile(options.file, options.doc, function (err) {
            if (err) { console.log(err); }
            if(cb) cb();
        });
    };
};

function recursive(path,next){
    return makeParentDir(path,next);
}
function makeParentDir(path,next){
    // recursively make sure that directory exists.
    if(/(.*?)\/\w+$/.exec(path)){
        console.log('recursing: ',path);
        fs.stat(path,function(err,stats){
            if(err){
                console.log('no path ',path);
                return recursive(RegExp.$1,
                                 makeme(path,next));
            }else{
                console.log('have path, recusing ends at ',path);
                next();
            }

        });
    }else{
        console.log('in make parent dir, regex failed on : ',path);
    }
    return;
}


exports.file_caching_service = function file_caching_service(options){
    var root = process.connectEnv.staticRoot || options.root || process.cwd();
    var pathParams = options.pathParams || ['zoom','column'];
    var fileParam  = options.fileParam  || 'row';
    function getPath(req){
        var activeParams = pathParams.filter(function(a){return req.params[a];});
        var dirs = activeParams.map(function(a){return req.params[a];});
        var targetpath = [root,dirs.join('/')].join('/');
        return targetpath;
    }
    function getFile(req){
        var format = req.format || 'json';
        var filename = req.params[fileParam]+'.'+format;
        return filename;
    }

    return function file_caching_service(res,req){
        var oldend = res.end;

        var targetpath = getPath(req);
        var filename = [targetpath,getFile(req)].join('/');
        var localWriteEnd = function(doc){
            var writeOut = writeGeoJSON({file:filename,doc:doc});
            makeParentDir(targetpath,writeOut);
        };
        return function(chunk,encoding){
            res.end = oldend;
            res.end(chunk, encoding);
            console.log('backing out of the tile cacher with '+filename);
            localWriteEnd(chunk); // and then save to fs for next time
        };
    };
};


exports.couch_caching_service = function couch_caching_service(options){
    var root = process.connectEnv.staticRoot || options.root || process.cwd();
    var pathParams = options.pathParams || ['zoom','column'];
    var fileParam  = options.fileParam  || 'row';
    var couchdb_arg = [options.port, options.host, options.user, options.pass];

    function connectToCouch(next){
        var client = couchdb.createClient(options.port, options.host, options.user, options.pass);
        var db = client.db(options.db);

        next(null,db);
    }

    function couchGeoJSON(c,next){
        var buffer=[];
        for(var index in c.doc){
            // features
        return function(err,db){
            if (err) next( new Error(JSON.stringify(er)));
            db.bulkDocs({
                docs: buffer
            }, function(er, r) {
                if (er) next(new Error(JSON.stringify(er)));
            });
        };
    }

    function getPath(req){
        var activeParams = pathParams.filter(function(a){return req.params[a];});
        var dirs = activeParams.map(function(a){return req.params[a];});
        var targetpath = [root,dirs.join('/')].join('/');
        return targetpath;
    }
    function getFile(req){
        var format = req.format || 'json';
        var filename = req.params[fileParam]+'.'+format;
        return filename;
    }

    return function couch_caching_service(res,req,next){
        var oldend = res.end;

        var targetpath = getPath(req);
        // var filename = [targetpath,getFile(req)].join('/');
        var localWriteEnd = function(doc){
            var writeOut = couchGeoJSON({
                'targetpath':targetpath
                ,'filename':filename
                ,'doc':doc
            }, next);
            connectToCouch(writeOut);
        };
        return function(chunk,encoding){
            res.end = oldend;
            res.end(chunk, encoding);
            console.log('backing out of the tile couch cacher with '+filename);
            localWriteEnd(chunk); // and then save to couchdb for next time
        };
    };
};

