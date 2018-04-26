# Spine Web API

This module provides the web API for a Spine application.

The entry points of the API are `CommandServlet` and `QueryServlet`, which proxy actor requests to 
the corresponding Spine services.

## Delivering Query Results

In order to deliver results of a query to the client, a `QueryServlet` uses a `QueryMediator`.
This is an implementation-specific component, which oversees the client responses.

See `firebase-web` library for a `QueryMediator` implementation.
