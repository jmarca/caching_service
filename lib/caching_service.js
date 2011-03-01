var fs = require('fs');
var couchdb = require('couchdb');
var crypto = require('crypto');

function makeme (dir,cb){
    return function(){
        // console.log('makeme: ',dir);
        fs.mkdir(dir,0777,function(err){
            if(err) { }
            if(cb) cb();
        });
    };
};

function writeGeoJSON(options,cb){
    return function(){
        fs.writeFile(options.file, options.doc, function (err) {
            if (err) { }
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
        // console.log('recursing: ',path);
        fs.stat(path,function(err,stats){
            if(err){
                // console.log('no path ',path);
                return recursive(RegExp.$1,
                                 makeme(path,next));
            }else{
                // console.log('have path, recusing ends at ',path);
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

    return function file_caching_service(req,res,next){
        var oldend = res.end;

        var targetpath = getPath(req);
        var filename = [targetpath,getFile(req)].join('/');
        var localWriteEnd = function(doc){
            var writeOut = writeGeoJSON({file:filename,doc:doc});
            makeParentDir(targetpath,writeOut);
        };
        res.end =  function(chunk,encoding){
            res.end = oldend;
            res.end(chunk, encoding);
            //console.log('backing out of the tile cacher with '+filename);
            localWriteEnd(chunk); // and then save to fs for next time
        };
        next();
    };
};

var env = process.env;


exports.couch_cache_docs = function couch_cache_docs(options){
    var root =  options.root ?  options.root : process.connectEnv.staticRoot ? process.connectEnv.staticRoot : process.cwd();
    var unique_doc_properties = options.unique_doc_properties ? options.unique_doc_properties : ['ts','components'];
    var pathParams = options.pathParams ? options.pathParams : ['zoom','column'];
    var fileParam  = options.fileParam ;

    function connectToCouch(next){
        var client = couchdb.createClient(options.port, options.host, options.user, options.pass);
        var db = client.db(options.db);

        next(null,db);
    }

    /**
     * couchBulk -- save data using the couchdb bulk docs api
     * parameters
     *
     *  c -- an object containing
     *
     *  c.targetpath : an object with details about the query path (for example,
     *     :year/:zoom/:column/:row type url would generate {year:2008,
     *     zoom:3, row:3, column3}
     *
     *  c.root_id : a string that will serve as the root document id
     *     for all documents being saved.  It will be combined with
     *     the unique_properties of each document to generate the
     *     unique id for couchdb.
     *
     *  c.unique_properties : an array holding the unique properties
     *     in the doc that should be appended to the doc id formed by the
     *     query path
     *
     *  c.docs : an array holding the documents to save
     *
     *  c.docs[0], etc : each document is an object that must have an
     *     element feature, which is an object containing at least an
     *     element properties, with entries for each of the
     *     c.unique_properties.  For example, if c.unique_properties =
     *     ['ts','components'], the at a minimum the document object
     *     should look like
     *     doc={'feature':{'properties':{'ts':something,'components':[some,other,thing]}}};
     *
     **/
    function couchBulk(c,next){
        var buffer = [];
        for (var d in c.docs){
            var features = c.docs[d];
            for (var f in features){
                var doc = {};
                for(var k in c.targetpath){ doc[k] = c.targetpath[k] ; }
                doc.feature = features[f];
                var unique_bits = c.unique_properties.map(function(p){return doc.feature.properties[p];});
                doc._id = [c.root_id,unique_bits].join('/');
                buffer.push(doc);
            }
        }
        return function(err,db){
            if (err){
                console.log('error: '+JSON.stringify(err));
                next( new Error(JSON.stringify(err)));
                return;
            }
            next(null,c.docs);

            db.bulkDocs({
                docs: buffer
            }, function(er, r) {
                if (er){
                    console.log('error: '+JSON.stringify(er));
                }
                else{
                   // console.log('done with bulk docs, r is '+JSON.stringify(r));
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
                ,'unique_properties' : unique_doc_properties
                ,'root_id' : targetpath._id
                ,'docs':docs
            },next);
            connectToCouch(writeOut);
        };
        return function(err,docs){
            if (err){
                console.log('error: '+JSON.stringify(err));
                next( err );
                return;
            }
            //console.log('direct call to couch cache with '+filename+' and docs '+JSON.stringify(docs));
            localWriteEnd(docs);
        };
    };
};

exports.couch_get_docs = function couch_get_docs(options){
    var root =  options.root ?  options.root : process.connectEnv.staticRoot ? process.connectEnv.staticRoot : process.cwd();
    var unique_doc_property = options.unique_doc_property ? options.unique_doc_property : 'components';
    var pathParams = options.pathParams ? options.pathParams : ['zoom','column'];
    var fileParam  = options.fileParam ;

    var design = options.design ? options.design : 'zcr';
    var view   = options.view   ? options.view   : 'zcr_y';

    function connectToCouch(next){
        var client = couchdb.createClient(options.port, options.host, options.user, options.pass);
        var db = client.db(options.db);
        next(null,db);
    }

    function couchView(c,next){
        // console.log('setting up view query '+JSON.stringify(c));
        return function(err,db){
            if (err){
                console.log('error: '+JSON.stringify(err));
                next( new Error(JSON.stringify(err)));
                return;
            }
            db.view(
                design,view,c
                , function(er, r) {
                    if (er){
                        console.log('error: '+JSON.stringify(er));
                    }
                    else{
                        //console.log('done with query, r is '+JSON.stringify(r));
                    }
                    var features={};
                    for (var row_i in r.rows){
                        var row = r.rows[row_i];

                        if(!features[row.value.properties[unique_doc_property]]){
                            features[row.value.properties[unique_doc_property]] = [];
                        }
                        features[row.value.properties[unique_doc_property]].push(row.value);
                    }
                    next(null,features);
                });
        };
    }

    function getViewQuery(req){

        var activeParams = pathParams.filter(function(a){return req.params[a];});
        var startkey=[];
        var endkey=[];
        var last = activeParams[activeParams.length - 1];

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

        var query = getViewQuery(req);

        return function(err){
            if (err){
                console.log('error: '+JSON.stringify(err));
                next( err );
                return;
            }
            var fetch = couchView(query ,next);
            connectToCouch(fetch);
        };
    };
};

