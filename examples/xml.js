var xml2db = require('../parser/xml2db');

xml2db.getStream('./import/sitemap.xml', function(err, stream, fileName){
    var converter = new xml2db(stream, fileName);
    converter.import();
    //converter.synchronize();
    //converter.export('./export/sitemap.xml');
    //converter.clean();
});