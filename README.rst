Features Supported
==================

DynamoDB supported features:

  * *batched operations* - the current DynamoDB API only supports batched
    fetch operations. Pynamo mixes that with `get_or_create_many` to create
    a batched operation facade.
  * *set operations* - DynamoDB has native support for sets of strings or
    numbers.
  * *UpdateItem* - Pynamo automatically uses `UpdateItem` when it makes sense.

Pynamo builds on the great `boto` package, whose underlying connection api
automatically uses a connection pool and supports keep-alive and timeouts.

Additionally, Pynamo comes with some features of it's own:
  
  * **declaritive schema** - Pynamo's schema goes beyond defining just a hash key
    and range key. 
  * *compound keys* keys that are composed of multiple keys.
  * *synthesized types* attributes that can be container types like `list`
    or `dict`


Running the tests
=================

First, you will need an Amazon AWS account. If you don't have one of those, why
are you running these tests?

Create a file: `~/.pynamo.cfg` which provides the following data (filled in)::

    [aws]
    access_key_id = 
    secret_access_key = 

    [dynamodb]
    table_prefix = 

I use `nose` which pretty much rocks. From the main repository directy simply 
run `nosetests` which will pick up all the Pynamo tests and run them.

Running the full test suite takes a very long time. It has to create and destroy
many DynamoDB tables, and at the time of writing that seems to take quite a 
while.

If some tests fail, you may have to delete them from the AWS Console or another
command line client by hand.
