===================
API and Schema
===================
---
API
---
..  class:: Connection
     
    .. method:: connect{host='127.0.0.1', port=33015, timeout=15, schema={}}
        
        Connect to tarantool instance and create connection object.
        You may use it like:
        
        .. code-block:: lua
            
            Connection.connect{host='preffered.host', port=1337, schema=MYSCHEMA}
            -- OR
            Connection{host='preffered.host', port=1337, schema=MYSCHEMA}
        
        :type host: string
        :param host: string with tarantool address
        :type port: number
        :param port: tarantool primary port (or secondary)
        :type timeout: number
        :param timeout: connection timeout (in seconds)
        :type schema: table
        :param schema: schema for tarantool connection
        :return: Connection object of Tarantool
    
    ..  method:: close()
        
        Close connection to Tarantool.
    
    ..  method:: error(string = nil, level = 1)
        
        Function for throwing Tarantool error.
        
        If error is nil, then program exits without message of error.
        
        :param string: error string
        :param level:  level number
        :type string:  string
        :type level:   number
        :return: throws error
    
    ..  method:: insert (space_no, ...)
                 store  (space_no, ...)
                 replace(space_no, ...)
        
        Insert, Store or Replace a tuple in(to a) space. 
        
        Tuple fields follow space_no. 
        
        If a tuple with the same primary key already exists, insert()
        returns an error, but replace() replaces it.
        If a tuple with the same primary key not exists, replace() 
        return and error, but insert() inserts new.
        Store will insert/replace tuple no matter what.
        
        .. code-block:: lua
        
            conn:insert(0, 'hello', 42)
            -- OR
            conn:insert(0, {'hello', 42})
            -- Both valid and identical
        
        :param space_no: number of space to insert tuple into.
        :type space_no: number 
        :param ...: tuple to insert/replace/store
        :type ...: table with strings/number or vararg'd strings/numbers.
        :return: (true, table) if request was OK. Table with inserted tuple(s).
        :return: (false, string) if request can't be processed right now. String with error.
        :return: throws error if there was an error in request
        
    .. method:: select(space, index, keys[, offset=0[, limit=0xFFFFFFFF]])
        
        Search for a tuple or tuples in the given space.
        
        .. code-block:: lua
            
            -- Next three are identical
            conn:select(0, 1, 'hello')
            conn:select(0, 1, {'hello'})
            conn:select(0, 1, {{'hello'}})
            -- Next two are identical
            conn:select(0, 1, {'hello', {'world'})
            conn:select(0, 1, {{'hello'}, {'world'})
            -- And this two aren't
            conn:select(0, 1, {'hello', 'world')
            conn:select(0, 1, {{'hello'}, {'world'})
            -- This query tries to select all tuples (if index 1 of space 0 is TREE, else error is thrown)
            conn:select(0, 1, {})
        
        :param space_no: number of space to delete tuple from
        :type space_no: number
        :param index: number of index where key must be searched
        :type index: number
        :param key: keys to select tuple with
        :type key: table of tables of strings/numbers(many keys) or table with strings/numbers(multipart keys) or string/number
        :return: (true, table) if request was OK. Table with selected tuple(s).
        :return: (false, string) if request can't be processed right now. String with error.
        :return: throws error if there was an error in request
         
    .. method:: delete(space_no, ...)
        
        Delete a tuple identified by a primary key.
        
        :param space_no: number of space to delete tuple from.
        :type space_no: number
        :param ...: primary key to delete tuple with.
        :type ...: table with strings/number or vararg'd strings/numbers.
        :return: (true, table) if request was OK. Table with deleted tuple(s).
        :return: (false, string) if request can't be processed right now. String with error.
        :return: throws error if there was an error in request
    
    .. method:: update(space_no, key, ops)
        
        Update a tuple identified by a primary key. If a key is multipart,
        it is passed in as a Lua table.
        
        Operations:
        
        * {'set'   , position, value}  - set value in positition `position` to `value` (or '=')
        
        * {'add'   , position, number} - add `value` to field in position `position` (or '+')
        
        * {'and'   , position, number} - binary and `value` to field in position `position` (or '&')
        
        * {'xor'   , position, number} - binary xor `value` to field in position `position` (or '^')
        
        * {'or'    , position, number} - binary or `value` to field in position `position` (or '|')
        
        * {'splice', position, from, to, insert} - cut value on position `position` from `from` and up to `to`,then insert `insert` in the middle. (or ':')
        
        * {'delete', position} - delete value in the position `position` (or '#', or 'del')
        
        * {'insert', position, value} - insert `value` before the `position` (or '!', or 'ins') 
        
        .. code-block:: lua
            
            conn:update(0, {'hello', 'world'}, {{'set', 1, 'some new field'}})
            conn:update(0, {'hello', 'world'}, {{'=', 1, 234}})
            conn:update(0, {'hello', 'world'}, {{'+', 2, 234}})
            -- ...
            conn.splice(0, 'hi, matthew', {{'+', 1, 234}, {'^', 1, 234}})
                
        :param space_no: number of space to delete tuple from
        :type space_no: number
        :param key: primary key to delete tuple with
        :type key: table with strings/numbers(multipart key) or string or number
        :param ops: table with operations (as described upper)
        :type ops: `value` may be number or string, `from` is number, `to` is number, `insert` is string, `position` is number
        :return: (true, table) if request was OK. Table with updated tuple(s).
        :return: (false, string) if request can't be processed right now. String with error.
        :return: throws error if there was an error in request
    
    .. method:: call(name, ...)
        
        Call a remote stored procedure, such as box.select_reverse_range().
        
        .. code-block:: lua
            
            conn:call('box.select_range', 4, 1, 10)
            conn:call('box.time64')
        
        :param name: name of stored procedure
        :type name: string
        :param ...: primary key to delete tuple with.
        :type ...: table with strings/number or vararg'd strings/numbers [may be empty]
        :return: (true, table) if request was OK. Table with returned arguments.
        :return: (false, string) if request can't be processed right now. String with error.
        :return: throws error if there was an error in request
    
    .. method:: ping()
     
        Ping Tarantool server.
         
        :return: (true, number) if request was OK. Number - is time to response for server.
        :return: (false, string) if request can't be processed right now. String with error.
        :return: throws error if there was an error in request.
    
    ..  method:: _reqid()
        
        Function for counter ID.
        
        Increments and returns value. Uses self._req_id for storing values.
        0 in the begging.
        
        You may redefine this function for using your function (when
        you use coroutines or threads for syncing for example.)
        
        :return: Request ID
        :rtype:  Number


------------------
Schema Description
------------------

Typical schema looks like:

.. code-block:: lua

    SCHEMA = {
        spaces = {
            [0] = {
                fields  = {'string', 'number32'},
                indexes = {
                    [0] = {0},
                    [1] = {1, 2},
                }
            ...
        },
        funcs = {
            'queue.put' = {
                [from] = {...},
                [to]   = {'string', 'number64',...},
            },
            ...
        }
    }

In "spaces" field must be table of "space number" : "space specification"
Space specification includes fields(table of types) and indexes
(table of "index number" : "index fields"). Index fields is table
of fields number for current index.

In "funcs" fields must be table of "function name" : "table of input
arguments and table of return values". Both of them are table with types.

Types may be: 'string', 'number32', 'number64'.

This module is not intended to use by user,
but format of package is described here.
