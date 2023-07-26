import streamlit as st
from supabase_connection import SupabaseConnection
import time

st.title('Supabase Connection Demo')
st.write('This [Streamlit Connections Hackathon](https://discuss.streamlit.io/t/connections-hackathon/47574) project is brought to you by [Jay Pinho](https://twitter.com/jaypinho).')

# The first time the page loads, use a TTL of 10 minutes
conn = st.experimental_connection("supabase", type=SupabaseConnection)
df = conn.read('contacts', ttl=600)
# Add a checkbox column for deleting selected rows
df.insert(0, 'Select', [False] * len(df))

data_container = st.empty()
data_editor = data_container.data_editor(df, disabled=[x for x in df.columns.values if x != 'Select'])

if st.button('Delete Selected Row(s)', disabled=len(data_editor) == 0 or len(list(data_editor.loc[data_editor['Select'], 'id'])) == 0):
    conn.delete('contacts', [{'id': list(data_editor.loc[data_editor['Select'], 'id']), 'comparison': 'in_'}]) # See https://github.com/supabase-community/postgrest-py/blob/3bda9534a630600538f45ddc34d9c7eebdb19767/postgrest/base_request_builder.py#L325 for underscore
    df = conn.read('contacts', ttl=0)
    df.insert(0, 'Select', [False] * len(df))
    data_editor = data_container.data_editor(df, disabled=[x for x in df.columns.values if x != 'Select'])

with st.form("insert_row"):
    first_name = st.text_input('First name')
    last_name = st.text_input('Last name')
    age = st.slider('Age', min_value=1, max_value=110, value=20)

    submitted = st.form_submit_button(label='Insert Row')
    if submitted:
        conn.write('contacts', [{'first_name': first_name.strip(), 'last_name': last_name.strip(), 'age': age}])
        df = conn.read('contacts', ttl=0)
        df.insert(0, 'Select', [False] * len(df))
        data_editor = data_container.data_editor(df, disabled=[x for x in df.columns.values if x != 'Select'])


st.write('Although it is not demonstrated here, the Supabase Connection allows you to `update` existing data rows as well.')