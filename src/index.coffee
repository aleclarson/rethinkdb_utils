
rethinkdb = require "rethinkdb"

db = null

exports.connect = (name) ->
  db = rethinkdb {db: name}
  return

exports.createTable = (table) ->
  db.tableCreate table
    .run()

exports.createIndex = (index) ->
  parts = index.split "."
  if parts.length isnt 2
    throw Error "Must format index like 'table.index'"
  db.table parts[0]
    .indexCreate parts[1]
    .run()

exports.renameField = (oldField, newField) ->
  parts = oldField.split "."
  if parts.length isnt 2
    throw Error "Must format first field like 'table.field'"
  table = db.table parts[0]
  copyField = table.update mapField newField, parts[1]
  removeField = table.replace (row) -> row.without parts[1]
  db.expr [copyField, removeField]
    .run()

exports.deleteField = (field) ->
  parts = field.split "."
  if parts.length isnt 2
    throw Error "Must format field like 'table.field'"
  db.table parts[0]
    .replace (row) -> row.without parts[1]
    .run()

mapField = (a, b) ->
  map = {}
  map[a] = db.row b
  return map

