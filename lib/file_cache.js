var fs = require('fs');
var crypto = require('crypto');
var makedir = require('makedir').makedir;

function writeGeoJSON(options,cb){
    return function(){
        fs.writeFile(options.file, options.doc, function (err) {
            if (err) { }
            if(cb) cb();
        });
    };
};


exports.file_caching_service = function file_caching_service(options){
    var root =  options.root || process.cwd();
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
            makedir(targetpath,writeOut);
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
