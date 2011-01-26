var fs = require('fs');
var couchdb = require('couchdb');
var crypto = require('crypto');

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

var env = process.env;


exports.couch_cache_docs = function couch_cache_docs(options){
    var root = process.connectEnv.staticRoot || options.root || process.cwd();
    var pathParams = options.pathParams || ['zoom','column'];
    var fileParam  = options.fileParam  || 'row';
    var user = options.user || env.COUCHDB_USER;
    var pass = options.user || env.COUCHDB_PASS;

    var couchdb_arg = [options.port, options.host, options.user, options.pass];

    function connectToCouch(next){
        var client = couchdb.createClient(options.port, options.host, options.user, options.pass);
        console.log('created client');
        var db = client.db(options.db);
        console.log('created db');

        next(null,db);
    }

    function couchBulk(c,next){
        var buffer = [];
        console.log('setgin up buffer for '+JSON.stringify(c));
        for (var d in c.docs){
            console.log('processing '+d);
            var features = c.docs[d];
            for (var f in features){
                console.log('processing '+d + ' feature '+f);
                var doc = {};
                for(var k in c.targetpath){ doc[k] = c.targetpath[k] ; }
                doc.feature = features[f];
                doc._id = [doc._id,doc.feature.properties.ts,doc.feature.properties.detector_id].join('/');
                buffer.push(doc);
            }
        }
        console.log('done returning callback');
        return function(err,db){
            console.log('calling couch with buffer'+JSON.stringify(buffer));
            if (err){
                console.log(JSON.stringify(err));
                next( new Error(JSON.stringify(err)));
                return;
            }
            next(null,c.docs);

            db.bulkDocs({
                docs: buffer
            }, function(er, r) {
                if (er){
                    console.log(JSON.stringify(er));
                }
                else{
                    console.log('done with bulk docs, r is '+JSON.stringify(r));
                }
            });
        };
    }

    function getPath(req){
        var activeParams = pathParams.filter(function(a){return req.params[a];});
        var dirs={};
        activeParams.map(
            function(q,r){
                dirs[q] =  req.params[q];
                dirs._id = dirs._id ?  [dirs._id, req.params[q]].join('/') : req.params[q];
            }
        );

        console.log(JSON.stringify(dirs));
        return dirs;
    }
    function getFile(req){
        var format = req.format || 'json';
        var filename = req.params[fileParam]+'.'+format;
        return filename;
    }

    return function couch_cache_docs(req,next){

        var targetpath = getPath(req);

        var filename = getFile(req);
        targetpath.filename=filename;
        var localWriteEnd = function(docs){
            var writeOut = couchBulk({
                'targetpath':targetpath
                ,'docs':docs
            },next);
            connectToCouch(writeOut);
        };
        return function(err,docs){
            if (err){
                console.log(JSON.stringify(err));
                next( err );
                return;
            }
            console.log('direct call to couch cache with '+filename+' and docs '+JSON.stringify(docs));
            localWriteEnd(docs);
        };
    };
};

exports.couch_get_docs = function couch_get_docs(options){
    var root = process.connectEnv.staticRoot || options.root || process.cwd();
    var pathParams = options.pathParams || ['zoom','column'];
    var fileParam  = options.fileParam  || 'row';
    var user = options.user || env.COUCHDB_USER;
    var pass = options.user || env.COUCHDB_PASS;
    var design = options.design || 'zcr';
    var view = options.view || 'zcr_ym';

    var couchdb_arg = [options.port, options.host, options.user, options.pass];

    function connectToCouch(next){
        var client = couchdb.createClient(options.port, options.host, options.user, options.pass);
        console.log('created client');
        var db = client.db(options.db);
        console.log('created db');

        next(null,db);
    }

    function couchView(c,next){
        console.log('setting up view query '+JSON.stringify(c));
        return function(err,db){
            if (err){
                console.log(JSON.stringify(err));
                next( new Error(JSON.stringify(err)));
                return;
            }
            console.log('calling couch view');
            db.view(
                design,view,c
                , function(er, r) {
                    if (er){
                        console.log(JSON.stringify(er));
                    }
                    else{
                        console.log('done with query, r is '+JSON.stringify(r));
                    }
                    var features={};
                    for (var row_i in r.rows){
                        var row = r.rows[row_i];

                        if(!features[row.value.properties.components]){
                            features[row.value.properties.components] = [];
                        }
                        features[row.value.properties.components].push(row.value);
                    }
                    console.log('\ncache fetch got:  '+JSON.stringify(features));
                    next(null,features);
                });
        };
    }

    function getPath(req){
        console.log(pathParams.join(' '));
        var activeParams = pathParams.filter(function(a){return req.params[a];});
        var startkey=[];
        var endkey=[];
        var last = activeParams[activeParams.length - 1];
        console.log(activeParams.join(' '))
        activeParams.map(
            function(q){
                startkey.push(req.params[q]);
                if (q != last){
                    endkey.push(req.params[q]);
                }else{
                    endkey.push({});
                }
            }
        );

        return {'startkey':startkey,'endkey':endkey};

    }

    return function couch_get_docs(req,next){

        var targetpath = getPath(req);

        return function(err){
            if (err){
                console.log(JSON.stringify(err));
                next( err );
                return;
            }
            var fetch = couchView(targetpath ,next);
            connectToCouch(fetch);
        };
    };
};

