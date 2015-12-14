/**
 * Created by Anton Gnyady.
 * API:
 *
 * #on('startElement' function (name, attrs) {})
 * #on('endElement' function (name) {})
 * #on('text' function (text) {})
 * #on('processingInstruction', function (target, data) {})
 * #on('comment', function (s) {})
 * #on('xmlDecl', function (version, encoding, standalone) {})
 * #on('startCdata', function () {})
 * #on('startCdata', function () {})
 * #on('endCdata', function () {})
 * #on('entityDecl', function (entityName, isParameterEntity, value, base, systemId, publicId, notationName) {})
 * #on('error', function (e) {})
 * #stop() pauses
 * #resume() resumes
 */

var expat = require('node-expat');
var fs = require('fs');
var parsePath = require('path');
var url = require('url');
var validUrl = require('valid-url');
var http = require('http');

var xml = constructor;


//Options, example:
//var options =
//{
//    availableTags: ['urlset', 'url', 'loc', 'lastmod', 'priority', 'changefreq'],
//    maxAttributes: 3,
//    availableAttr: {urlset: ['xml_nsxsi', 'xsi_schemaLocation', 'xmlns']}
//};


function constructor(stream, options) {
    var self = this;
    if (typeof options == 'object') {
        this.availableTags = options.availableTags;
        this.maxAttributes = options.maxAttributes;
        this.availableAttr = options.availableAttr;
        this.encoding = options.encoding;
    }
    this.encoding = this.encoding ? this.encoding : 'UTF-8';
    this.parser = new expat.Parser(this.encoding);
    this.stream = stream;

    this.stream.on('data', function (data) {
        if (!self.parser.parse(data, false)) {
            self.parser.emit('error', new Error(self.parser.getError() + " in line " + self.parser.getCurrentLineNumber()));
        }
    });
    this.stream.on('end', function () {
        self.parser.parse('', true);
        self.parser.parser.emit('end');
    });

    if (this.availableTags || this.maxAttributes || this.availableAttr) {
        this.parser.on('startElement', function (name, attr) {
            if (self.availableTags) {
                var isAvailable = false;
                for (var i = 0; i < self.availableTags.length; i++) {
                    if (self.availableTags[i] === name) {
                        isAvailable = true;
                        break;
                    }
                }
                if (!isAvailable) {
                    self.pause();
                    self.parser.emit('error', new Error("Unknown tag in line " + self.parser.getCurrentLineNumber()));
                    self.destroy();
                }
            }


            if (self.maxAttributes && self.maxAttributes < Object.keys(attr).length) {
                self.pause();
                self.parser.emit('error', new Error("Limit Exceeded of attributes count at line " + self.parser.getCurrentLineNumber()));
                self.destroy();
            }

            if (self.availableAttr && self.availableAttr[name]) {
                for (var at in attr) {
                    isAvailable = false;
                    for (i = 0; i < self.availableAttr[name].length; i++) {
                        if (self.availableAttr[name][i] === at) {
                            isAvailable = true;
                            break;
                        }
                    }
                    if (!isAvailable) {
                        self.pause();
                        self.parser.emit('error', new Error("Unknown attribute in line " + self.parser.getCurrentLineNumber()));
                        self.destroy();
                        break;
                    }
                }
            }
        });
    }
}

xml.getStream = function getStream(path, callback) {
    if (validUrl.isUri(path)) {
        var options = {
            host: url.parse(path).host,
            port: 80,
            path: url.parse(path).pathname
        };

        var ext = parsePath.extname(path);
        var fileName = parsePath.basename(path, ext);

        http.get(options, function (stream) {
            callback(undefined, stream, fileName);
        });
    } else {
        var ext = parsePath.extname(path);
        var fileName = parsePath.basename(path, ext);
        var fsStream = fs.createReadStream(path);
        callback(undefined, fsStream, fileName);
    }
};

xml.prototype.on = function (eventType, callback) {
    this.parser.on(eventType, callback);
};

xml.prototype.pause = function () {
    this.parser.stop();
};

xml.prototype.resume = function () {
    this.parser.resume();
};

xml.prototype.setEncoding = function (encoding) {
    this.parser.setEncoding(encoding);
};

xml.prototype.destroy = function () {
    this.parser.destroy();
};

module.exports = xml;

//-----------TEST----------------
//getStream('./sitemap.xml', function (err, stream, fileName) {
//    var re = /[\s]/;
//    var p = new xml(stream, options);
//    p.on('startElement', function (name, attr) {
//        console.log('Start element: ' + name);
//    });
//
//    p.on('text', function (text) {
//        if (!re.test(text))
//            console.log(text);
//    });
//
//    p.on('endElement', function (name) {
//        console.log('End element: ' + name);
//    });
//
//    p.on('error', function (err) {
//        console.error(err);
//    });
//});