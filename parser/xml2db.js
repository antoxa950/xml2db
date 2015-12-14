/**
 * Created by Anton Gnyady.
 */

var xml = require('./xml');
var mysql = require('mysql');
var fs = require('fs');

var xml2db = constructor;

function constructor(stream, tableName, parserOptions) {
    this.stream = stream;
    this.tableName = tableName;
    this.db = mysql.createConnection({
        host: '91.234.26.48',
        user: 'anton',
        password: 'qwerty',
        database: 'sbase',
        multipleStatements: true
    });
    this.parserOptions = parserOptions;
    this.db.connect();
}

xml2db.prototype.finish = function(){
    this.db.end();
};

function isEmptyObject(obj) {
    if (!obj)
        return true;
    for (var tmp in obj) {
        return false;
    }
    return true;
}

function getNextRows(callback) {
    var self = this;
    var indexes = [];
    var quantityRequests = 0;
    var qRows = "select * from " + this.tableName + " where _orderID>=" + this.currPos + " order by _orderID limit " + this.rowCount + ";";

    for (var it in this.attributes) {
        if (this.currPos <= this.attributes[it].tableRows) {
            qRows += "select * from " + it + " where uID>=" + this.currPos + " order by uID limit " + this.rowCount + ";";
            indexes[it] = ++quantityRequests;
        }
    }
    this.currPos += this.rowCount;

    this.db.query(qRows, undefined, function (err, data) {
        if (quantityRequests === 0) {
            self.mainTable = self.mainTable.concat(data);
        } else if (quantityRequests > 0) {
            self.mainTable = self.mainTable.concat(data[0]);
            for (var it in indexes) {
                self.attributes[it].rows = self.attributes[it].rows.concat(data[indexes[it]]);
            }
        }
        if (callback) {
            callback.call(self);
        }
    });
}

xml2db.prototype.import = function (callback) {
    var qCreateTable = "create table if not exists " + this.tableName + "(_orderID int primary key, _uID int, _parentID int, tag varchar(32) default '', txt text, isEmpty bool default true) ENGINE=MyISAM;";
    var self = this;
    this.db.query(qCreateTable, undefined, function (err) {
        if (err) {
            console.log(err);
        } else {
            console.log("Created new table.");
        }
        body();
    });

    function body() {
        self.xml = new xml(self.stream, self.parserOptions);
        var depth = [];
        var qInsertValues = "insert into " + self.tableName + " values";
        var orderID = 0;
        var uID = 0;
        var queries = [];

        self.xml.on('startElement', function (attrName, attr) {
            if (attrName) {
                var parentId = null;
                var parentIndex = depth.length - 1;
                if (depth[parentIndex] && depth[parentIndex].uID) {
                    parentId = depth[parentIndex].uID;
                    depth[parentIndex].isEmpty = false;
                }

                var obj = {
                    orderID: ++orderID,
                    uID: ++uID,
                    parentID: parentId,
                    tag: attrName,
                    txt: '',
                    isEmpty: true
                };

                if (!isEmptyObject(attr)) {
                    var newTableName = self.tableName + '_' + attrName;
                    var tmp;
                    var isNew = false;
                    if (!queries[newTableName]) {
                        isNew = true;
                        tmp = queries[newTableName] = {
                            qCreate: "create table " + newTableName + "(uID int,",
                            qInsert: [],
                            fields: []
                        };
                        for (var field in attr) {
                            tmp.qCreate += field + ' text,';
                            tmp.fields.push(field);
                        }
                    } else {
                        tmp = queries[newTableName];
                    }
                    if (!isNew) {
                        for (var f in attr) {
                            var hasAt = false;
                            for (var i = 0; i < tmp.fields.length; i++) {
                                if (tmp.fields[i] === f) {
                                    hasAt = true;
                                    break;
                                }
                            }
                            if (!hasAt) {
                                tmp.qCreate += f + ' text,';
                                tmp.fields.push(f);
                            }
                        }
                    }
                    var values = [];
                    values.push(uID);
                    for (i = 0; i < tmp.fields.length; i++) {
                        if (tmp.fields[i] in attr) {
                            values.push(attr[tmp.fields[i]]);
                        } else {
                            values.push(null);
                        }
                    }
                    tmp.qInsert.push(values);
                }
                depth.push(obj);
            }
        });

        self.xml.on('text', function (text) {
            if (text) {
                depth[depth.length - 1].txt += text.replace(/(^\s*)|(\s*)$/g, '');
            }
        });


        self.xml.on('error', function (e) {
            console.log(e);
        });

        self.xml.on('endElement', function (data) {
            if (data) {
                var row = depth.pop();
                qInsertValues += "(" + row.orderID + ", " + row.uID + ", " + row.parentID + ", '" + row.tag + "', '" + row.txt + "', " + row.isEmpty + "),";
            }
        });

        self.xml.on('end', function () {
            qInsertValues = qInsertValues.slice(0, -1) + ';';
            for (var tmp in queries) {
                qInsertValues += queries[tmp].qCreate.slice(0, -1) + ') ENGINE=MyISAM;';
                for (var i = 0; i < queries[tmp].qInsert.length; i++) {
                    qInsertValues += "insert into " + tmp + " values(";
                    var miniQ = '';
                    for (var j = 0; j < queries[tmp].fields.length + 1; j++) {
                        if (queries[tmp].qInsert[i][j]) {
                            miniQ += "'" + queries[tmp].qInsert[i][j] + "',";
                        } else {
                            miniQ += "null,";
                        }
                    }
                    qInsertValues += miniQ.slice(0, -1) + ');';
                }
            }

            self.db.query(qInsertValues, undefined, function (err) {
                if (err) {
                    console.log(err);
                }
            });
            console.log("End parse.UID: " + uID + "\nQUERY:\n" + qInsertValues);
            if (callback) {
                callback();
            }
            self.db.end();
        });
    }
};

xml2db.prototype.synchronize = function (callback) {

    var self = this;

    this.rowCount = 1;
    this.currPos = 1;
    this.attributes = [];
    this.mainTable = [];

    var getTableSize = "SELECT ROUND(data_length/1024/1024,2) AS total_size_mb, table_rows FROM information_schema.tables WHERE table_schema=DATABASE() and table_name like '" + this.tableName + "';";
    var getAllAttributes = "select table_name, table_rows from information_schema.TABLES where table_name like '" + this.tableName + "_%';";

    this.db.query(getTableSize + getAllAttributes, undefined, function (err, data) {
        self.mainTable.countRows = data[0][0].table_rows;
        self.rowCount = data[0][0].table_rows;

        for (var i = 0; i < data[1].length; i++) {
            self.attributes[data[1][i].table_name] = {
                tableRows: data[1][i].table_rows,
                rows: []
            };
        }

        getNextRows.call(self, body);

    });

    function getRowById(mas, id) {
        for (var i = 0; i < mas.rows.length; i++) {
            if (mas.rows[i].uID === id) {
                return mas.rows[i];
            }
        }
    }

    function body() {

        this.xml = new xml(this.stream, this.parserOptions);
        var depth = [];
        var query = "";
        var uID = 0;

        this.xml.on('startElement', function (name, attr) {
            var dbRow = self.mainTable.shift();
            if (name && dbRow) {
                ++uID;
                var obj = {
                    uID: dbRow._uID,
                    tag: name,
                    txt: dbRow.txt,
                    txtInXml: ''
                };
                var tagTable = self.tableName + "_" + dbRow.tag;
                var a;
                if (self.attributes[tagTable]) {
                    a = getRowById(self.attributes[tagTable], dbRow._uID);
                } else if (!isEmptyObject(attr)) {
                    query += "create table " + tagTable + "(uID int,";
                    var body = "";
                    for (var i in attr) {
                        body += i + " text,";
                    }
                    query += body.slice(0, -1) + ") ENGINE=MyISAM;";
                    self.attributes[tagTable] = {
                        tableRows: 0,
                        rows: []
                    };
                }
                if (isEmptyObject(attr) && a) {
                    query += "delete from " + tagTable + " where uID=" + dbRow._uID + ";";
                } else if (!isEmptyObject(attr) && a) {
                    var updateBody = "";
                    var alterBody = "";
                    for (var i in attr) {
                        if (i in a) {
                            if (a[i] !== attr[i]) {
                                updateBody += i + "='" + attr[i] + "',";
                            }
                        } else {
                            alterBody += i + " text,";
                            updateBody += i + "='" + attr[i] + "',";
                        }
                    }

                    for (var i in a) {
                        if (!(i in attr) && a[i] !== null && i !== 'uID') {
                            updateBody += i + "=null,";
                        }
                    }

                    if (alterBody.length > 0) {
                        query += "alter table " + tagTable + " add " + alterBody.slice(0, -1) + ";";
                    }

                    if (updateBody.length > 0) {
                        query += "update " + tagTable + " set " + updateBody.slice(0, -1) + " where uID=" + dbRow._uID + ";";
                    }

                } else if (!isEmptyObject(attr) && !a) {
                    query += "insert into " + tagTable + " set uID=" + dbRow._uID + ", ";
                    var body = "";
                    for (var i in attr) {
                        body += i + "='" + attr[i] + "',";
                    }
                    query += body.slice(0, -1) + ";";
                }
                depth.push(obj);
            }
        });

        this.xml.on('text', function (text) {
            var last = depth.length - 1;
            if (text) {
                depth[last].txtInXml += text.replace(/(^\s*)|(\s*)$/g, '');
            }
        });

        this.xml.on('endElement', function (data) {
            if (data) {
                var row = depth.pop();
                if (row.txt !== row.txtInXml) {
                    query += "update " + self.tableName + " set txt='" + row.txtInXml + "' where _uID=" + row.uID + ";";
                }
            }
        });


        this.xml.on('error', function (err) {
            console.error(err);
        });

        this.xml.on('end', function () {
            if (query.length > 0) {
                self.db.query(query, undefined, function (err) {
                    if (err) {
                        console.log(err);
                    }
                });
            }
            console.log("End parse.UID: " + uID + "\nQUERY:\n" + query);
            if (callback) {
                callback();
            }
            self.db.end();
        });
    }
};

xml2db.prototype.export = function (pathToFile, callback) {

    var self = this;

    this.rowCount = 1;
    this.currPos = 1;
    this.attributes = [];
    this.mainTable = [];

    var getTableSize = "SELECT ROUND(data_length/1024/1024,2) AS total_size_mb, table_rows FROM information_schema.tables WHERE table_schema=DATABASE() and table_name like '" + this.tableName + "';";
    var getAllAttributes = "select table_name, table_rows from information_schema.TABLES where table_name like '" + this.tableName + "_%';";

    this.db.query(getTableSize + getAllAttributes, undefined, function (err, data) {
        self.mainTable.countRows = data[0][0].table_rows;
        self.rowCount = data[0][0].table_rows;

        for (var i = 0; i < data[1].length; i++) {
            self.attributes[data[1][i].table_name] = {
                tableRows: data[1][i].table_rows,
                rows: []
            };
        }
        getNextRows.call(self, body);
    });

    function getRowById(mas, id) {
        for (var i = 0; i < mas.rows.length; i++) {
            if (mas.rows[i].uID === id) {
                return mas.rows[i];
            }
        }
    }

    function body() {
        var writer = fs.createWriteStream(pathToFile, {flags: 'w'});
        var depth = [];
        var xml = "";

        for (var i = 0; i < this.mainTable.length;) {
            var curr = this.mainTable[i];
            var next = this.mainTable[i + 1];
            var tagTable = self.tableName + "_" + curr.tag;
            var attrRow;
            xml += "<" + curr.tag + " ";
            if (this.attributes[tagTable] && (attrRow = getRowById(this.attributes[tagTable], curr._uID) )) {
                for (var a in attrRow) {
                    if (attrRow[a] !== null && a !== 'uID') {
                        xml += a + '="' + attrRow[a] + '" ';
                    }
                }
            }
            xml += ">";
            if (curr.txt !== '') {
                xml += curr.txt;
            } else {
                xml += '\n';
            }

            if (!next || curr._parentID === next._parentID) {
                xml += "</" + curr.tag + ">\n";
            } else if (next && curr._uID === next._parentID) {
                depth.push("</" + curr.tag + ">\n");
            } else if (next && curr._uID !== next._parentID) {
                xml += "</" + curr.tag + ">\n" + depth.pop();
            }

            if (!next) {
                var endTag = depth.pop();
                while (endTag !== undefined) {
                    xml += endTag;
                    endTag = depth.pop();
                }
            }
            writer.write(xml);
            xml = "";
            this.mainTable.shift();
        }
        writer.end('');
        this.db.end();
        if (callback) {
            callback();
        }
    }
};

xml2db.prototype.clean = function(callback){
    var getAllTables = "select table_name from information_schema.TABLES where table_name like '" + this.tableName + "%';";
    var self = this;

    this.db.query(getAllTables, undefined, function (err, data) {
        var query = "drop table ";
        for(var i = 0; i < data.length; i++){
            query += data[i].table_name + ",";
        }
        query = query.slice(0, -1) + ";";
        self.db.query(query, undefined, function(err){
            if (err) {
                console.error(err);
            }
            if (callback) {
                callback();
            }
            self.db.end();
        });
    });
};

module.exports = xml2db;
module.exports.getStream = xml.getStream;