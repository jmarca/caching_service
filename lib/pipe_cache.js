var makedir = require('makedir').makedir
var fs = require('fs')

exports.pipe_caching_service = function pipe_caching_service(options){
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

    return function pipe_caching_service(req,res,next){
        var oldend = res.end;
        var oldwrite = res.write;
        var targetpath = getPath(req);
        var filename = [targetpath,getFile(req)].join('/');
        var fstream;
        res.on('pipe',function(src){
            src.pipe(fstream);
        });
        makedir(targetpath,function(){
            fstream = fs.createWriteStream(filename);
            console.log('created fstream stream');
            fstream.on('close',function(){
                 console.log('handling fstream close');
            });
            next();
        });

    };
};
