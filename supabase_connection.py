import streamlit as st
from streamlit.connections import ExperimentalBaseConnection

from supabase import client, create_client, Client
import pandas as pd

class SupabaseConnection(ExperimentalBaseConnection[client.Client]):
    """Basic st.experimental_connection implementation for Supabase"""

    def _connect(self, **kwargs) -> client.Client:
        if 'url' in kwargs and 'key' in kwargs:
            url = kwargs.pop('url')
            key = kwargs.pop('key')
        else:
            url = self._secrets['supabase_url']
            key = self._secrets['supabase_key']
        return create_client(url, key)
    
    def cursor(self) -> client.Client:
        return self._instance

    # Example: conn.query('contacts', ttl=10)
    def query(self, table: str, ttl: int = 600) -> pd.DataFrame:
        @st.cache_data(ttl=ttl)
        def _query(table: str) -> pd.DataFrame:
            cursor = self.cursor()
            return pd.DataFrame(cursor.table(table).select("*").execute().data)
        
        return _query(table)
    
    # Example: conn.insert('contacts', [{'first_name': 'Jay', 'last_name': 'Jeffries', 'age': 36}])
    def insert(self, table: str, rows: list) -> pd.DataFrame:
        cursor = self.cursor()
        return pd.DataFrame(cursor.table(table).insert(rows).execute().data)
    
    # Example: conn.update('contacts', {'first_name': 'Joseph', 'age': 1}, [{'id': 10, 'comparison': 'lte'}])
    def update(self, table: str, updated_data: dict, conditions: list) -> pd.DataFrame:
        cursor = self.cursor()

        # Start with the new values you want to update the table with
        query_builder = cursor.table(table).update(updated_data)

        # Iterate over the list of conditions that dictate which rows get updated
        for condition in conditions:

            # Each list element is a dict. If this dict contains a 'comparison' key, then we need to use the right comparator (e.g. equal to, greater than, greater than or equal to, etc.).
            if 'comparison' in condition:
                condition_without_comparison = {k: v for k, v in condition.items() if k != 'comparison'}
                for k, v in condition_without_comparison.items():
                    method_call = getattr(query_builder, condition['comparison'])
                    query_builder = method_call(k, v)
            else:
                for k, v in condition.items():
                    query_builder = query_builder.eq(k, v)

        return pd.DataFrame(query_builder.execute().data)
    
    # Example: conn.delete('contacts', [{'id': 10, 'comparison': 'lte'}])
    def delete(self, table: str, conditions: list) -> pd.DataFrame:
        cursor = self.cursor()

        # Start the query builder
        query_builder = cursor.table(table).delete()

        # Iterate over the list of conditions that dictate which rows get deleted
        for condition in conditions:

            # Each list element is a dict. If this dict contains a 'comparison' key, then we need to use the right comparator (e.g. equal to, greater than, greater than or equal to, etc.).
            if 'comparison' in condition:
                condition_without_comparison = {k: v for k, v in condition.items() if k != 'comparison'}
                for k, v in condition_without_comparison.items():
                    method_call = getattr(query_builder, condition['comparison'])
                    query_builder = method_call(k, v)
            else:
                for k, v in condition.items():
                    query_builder = query_builder.eq(k, v)

        return pd.DataFrame(query_builder.execute().data)

    
# Create friendly aliases for common methods
setattr(SupabaseConnection, 'read', SupabaseConnection.query)
setattr(SupabaseConnection, 'write', SupabaseConnection.insert)
