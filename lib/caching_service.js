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




module.exports = function caching_service(options){
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

    function replaceEnd(res,filename,localWriteEnd,myend){
        return function(chunk,encoding){
            console.log('backing out of the tile cacher with '+filename);
            throw new Error('puk');
            if(chunk && chunk.length){
                console.log('have a chunk '+filename);

                localWriteEnd(chunk); // and then save to fs for next time
                end(chunk); // send that off
            }else{
                console.log('blowing chunks somewhere else along the line '+filename);
            }
            res.end = myend;
        };
    };
    var banana = 0;
    return function caching_service(res,req){
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

