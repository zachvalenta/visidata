from urllib.parse import urlparse, unquote
from visidata import VisiData, vd, Sheet, Column, asyncthread, AttrDict

__all__ = ['openurl_mongodb', 'openurl_mongo', 'MongoDatabasesSheet', 'MongoTablesSheet', 'MongoTable']


@VisiData.api
def openurl_mongodb(vd, url, filetype=None):
    """Open a MongoDB database connection.

    URL formats:
    - mongodb://[username:password@]host[:port] - lists all databases
    - mongodb://[username:password@]host[:port]/database[?options] - lists collections in database
    """
    pymongo = vd.importExternal('pymongo')

    url_parsed = urlparse(url.given)
    dbname = url_parsed.path.lstrip('/')

    # Build connection string
    if url_parsed.username:
        password = unquote(url_parsed.password) if url_parsed.password else ''
        auth = f"{url_parsed.username}:{password}@" if password else f"{url_parsed.username}@"
    else:
        auth = ''

    port = url_parsed.port or 27017
    host = url_parsed.hostname or 'localhost'

    # Handle query parameters - add directConnection=true if not already specified
    # This prevents pymongo from trying to discover replica set members with internal hostnames
    if url_parsed.query:
        query_params = url_parsed.query
        if 'directConnection' not in query_params:
            query_params += '&directConnection=true'
    else:
        query_params = 'directConnection=true'

    query = f"?{query_params}"

    connection_string = f"mongodb://{auth}{host}:{port}/{query}"

    # If no database specified, show list of databases
    if not dbname:
        return MongoDatabasesSheet(f"{host}_databases",
                                   connection_string=connection_string)

    # Otherwise, show collections in the specified database
    return MongoTablesSheet(f"{dbname}_collections",
                           connection_string=connection_string,
                           dbname=dbname)


# Alias for common abbreviation
VisiData.openurl_mongo = VisiData.openurl_mongodb


class MongoConnection:
    """Wrapper for MongoDB connection."""

    def __init__(self, connection_string, dbname=None):
        pymongo = vd.importExternal('pymongo')
        # Add timeouts to prevent hanging
        self.client = pymongo.MongoClient(
            connection_string,
            serverSelectionTimeoutMS=5000,  # 5 second timeout for initial connection
            connectTimeoutMS=10000,  # 10 second timeout for socket connection
            socketTimeoutMS=30000  # 30 second timeout for socket operations
        )
        self.connection_string = connection_string
        if dbname:
            self.db = self.client[dbname]
            self.dbname = dbname
        else:
            self.db = None
            self.dbname = None

    def list_databases(self):
        """Return list of database information."""
        return self.client.list_databases()

    def list_collections(self):
        """Return list of collection names."""
        if self.db is None:
            vd.fail('No database selected')
        return self.db.list_collection_names()

    def get_collection(self, name):
        """Get a collection by name."""
        if self.db is None:
            vd.fail('No database selected')
        return self.db[name]

    def close(self):
        """Close the connection."""
        if hasattr(self, 'client'):
            self.client.close()


class MongoDatabasesSheet(Sheet):
    """Sheet showing all databases on a MongoDB server."""

    rowtype = 'databases'

    def iterload(self):
        """Load list of databases from MongoDB server."""
        vd.status('Connecting to MongoDB...')
        self.mongo = MongoConnection(self.connection_string)

        # Get list of all databases
        try:
            vd.status('Fetching database list...')
            db_list = list(self.mongo.list_databases())
            vd.status(f'Found {len(db_list)} databases')
        except Exception as e:
            vd.exceptionCaught(e)
            vd.status(f'Error listing databases: {e}')
            return

        if not db_list:
            vd.warning('No databases found - check permissions')
            return

        # Set up columns
        self.columns = [
            Column('database_name', getter=lambda col, row: row['name']),
            Column('size_mb', type=float, getter=lambda col, row: round(row.get('sizeOnDisk', 0) / 1024 / 1024, 2)),
            Column('empty', type=bool, getter=lambda col, row: row.get('empty', False)),
        ]
        self.setKeys(self.columns[0:1])

        # Yield all database info rows
        for db_info in db_list:
            yield db_info

    def openRow(self, row):
        """Open a sheet showing collections in the selected database."""
        return MongoTablesSheet(f"{row['name']}_collections",
                               connection_string=self.connection_string,
                               dbname=row['name'])


class MongoTablesSheet(Sheet):
    """Sheet showing all collections in a MongoDB database."""

    rowtype = 'collections'

    def iterload(self):
        """Load list of collections from MongoDB database."""
        vd.status(f'Connecting to database {self.dbname}...')
        self.mongo = MongoConnection(self.connection_string, self.dbname)

        # Set up columns first
        self.columns = [
            Column('collection_name', getter=lambda col, row: row['name']),
            Column('doc_count', type=int, getter=lambda col, row: row.get('count', '?')),
            Column('indexes', type=int, getter=lambda col, row: row.get('indexes', '?')),
        ]
        self.setKeys(self.columns[0:1])

        vd.status('Fetching collection list...')
        coll_names = list(self.mongo.list_collections())
        vd.status(f'Found {len(coll_names)} collections')

        # Yield collections quickly without fetching stats (which can be slow)
        for i, coll_name in enumerate(coll_names):
            vd.status(f'Loading collection {i+1}/{len(coll_names)}: {coll_name}')

            # Create row with just the name first (fast)
            row = AttrDict(name=coll_name, count='?', indexes='?')

            # Try to get stats, but don't block if it's slow
            try:
                coll = self.mongo.get_collection(coll_name)
                row['count'] = coll.estimated_document_count()
                row['indexes'] = len(list(coll.list_indexes()))
            except Exception as e:
                vd.debug(f'Could not fetch stats for {coll_name}: {e}')

            yield row

    def openRow(self, row):
        """Open a sheet for the selected collection."""
        return MongoTable(f"{self.name}.{row['name']}",
                         mongo=self.mongo,
                         collection_name=row['name'])


class MongoTable(Sheet):
    """Sheet displaying documents from a MongoDB collection."""

    def iterload(self):
        """Load documents from MongoDB collection."""
        collection = self.mongo.get_collection(self.collection_name)

        # Fetch first document to infer schema
        first_doc = collection.find_one()
        if first_doc is None:
            return

        # Build columns from first document
        self.columns = []
        for key in first_doc.keys():
            self.addColumn(Column(key, getter=lambda col, row, key=key: self._get_value(row, key)))

        # Set _id as key column if it exists
        if '_id' in first_doc:
            self.setKeys([self.columns[0]])

        # Yield all documents
        for doc in collection.find():
            yield doc

    def _get_value(self, row, key):
        """Get value from document, handling nested structures."""
        value = row.get(key)

        # Convert ObjectId to string for display
        if hasattr(value, '__class__') and value.__class__.__name__ == 'ObjectId':
            return str(value)

        # Handle nested documents and arrays
        if isinstance(value, dict):
            return str(value)
        elif isinstance(value, list):
            return str(value)

        return value
