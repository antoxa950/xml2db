var xml2db = require('../parser/xml2db');

var options =
{
    availableTags: ['i', 'p','div', 'content', 'uri', 'name', 'contributor', 'email',
                    'feed', 'title', 'subtitle', 'updated',
                    'id', 'link', 'rights', 'generator', 'entry', 'published', 'author']
    //maxAttributes: 3,
    //availableAttr: {urlset: ['xml_nsxsi', 'xsi_schemaLocation', 'xmlns']}
};

xml2db.getStream('./import/atom.xml', function(err, stream, fileName){
    var converter = new xml2db(stream, fileName, options);
    converter.import();
    //converter.synchronize();
    //converter.export('./export/atom.xml');
    //converter.clean();
});