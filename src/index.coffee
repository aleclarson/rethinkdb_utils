
rethinkdb = require "rethinkdb"

db = null

exports.connect = (name) ->
  db = rethinkdb {db: name}
  return

exports.createTable = (table) ->
  db.tableCreate table
    .run()

exports.createIndex = (table, index) ->
  db.table table
    .indexCreate index
    .run()

exports.renameField = (table, oldField, newField) ->
  copyField = db.table(table).update mapField(newField, oldField)
  removeField = db.table(table).replace (row) -> row.without oldField
  db.do copyField, removeField
    .run()

mapField = (a, b) ->
  map = {}
  map[a] = db.row b
  return map

