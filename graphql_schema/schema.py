"""
Strawberry GraphQL schema definition for RosterIQ.

Combines Query, Mutation, and Subscription types into a single schema.
"""

import strawberry

from rosteriq.graphql_schema.queries import Query
from rosteriq.graphql_schema.mutations import Mutation
from rosteriq.graphql_schema.subscriptions import Subscription


# Create the GraphQL schema
schema = strawberry.Schema(
    query=Query,
    mutation=Mutation,
    subscription=Subscription,
)
