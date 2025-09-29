import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import JobSettings
import pandas as pd
import datetime
import os

headers = st.context.headers

workspace_url = os.environ.get("DATABRICKS_HOST")
user_access_token = headers.get("X-Forwarded-Access-Token")
user_email = headers.get("X-Forwarded-Email")
user = headers.get("X-Forwarded-User")
ip = headers.get("X-Real-Ip")

w = WorkspaceClient(token=user_access_token, auth_type="pat")

current_user = w.current_user.me()

if 'frequency' not in st.session_state:
    st.session_state['frequency'] = None

if 'selected_query_id' not in st.session_state:
    st.session_state['selected_query_id'] = None
if 'queries' not in st.session_state:
    queries = w.queries.list()
    labels = []
    values = []
    sqlText = []
    for q in queries:
        labels.append(q.display_name)
        values.append(q.id)
        sqlText.append(q.query_text) 

    st.session_state['queries'] = list(zip(values, values, sqlText))


daily_map = {
    "SUN": "Sunday",
    "MON": "Monday",
    "TUE": "Tuesday",
    "WED": "Wednesday",
    "THU": "Thursday",
    "FRI": "Friday",
    "SAT": "Saturday"
}

def parseIdAndNameDisplay(rawOption):
    whichQuery = w.queries.get(rawOption)
    return whichQuery.display_name

def getQueryText():
    query_id = st.session_state['selected_query_id']
    query = w.queries.get(query_id)
    query_object = query.as_dict()

col1, col2 = st.columns(2)

with col1:
    st.image("./img/Databricks_Logo.png", width=400)

with col2:
    st.image("./img/Excel_Logo.png", width=200)

st.title("Databricks Excel Report Mail Scheduler")

queries_df = pd.DataFrame(st.session_state['queries'], columns=['Id', 'Display Name', 'SQL Text'])

selected_query_id = st.selectbox(label="Which query would you like to run?",options=queries_df, format_func=parseIdAndNameDisplay, key="selected_query_id")

if st.session_state['selected_query_id']:
    st.write("Query Text:")
    query = w.queries.get(st.session_state['selected_query_id'])
    query_object = query.as_dict()
    st.code(query_object["query_text"], language='sql',)
    st.html("Need to test or edit your query? <a href='http://{0}/editor/queries/uuid/{1}' target='_blank'>Click here to open the SQL editor.</a>".format(workspace_url, st.session_state['selected_query_id']))

email_to = st.text_input("Send email to (comma-delimited addresses)")

selected_frequency = st.selectbox(
    label="How often should this run?", 
    options=["Daily", "Weekly", "Monthly"],
    placeholder="Select frequency...",
    index=None,
    key="frequency"
)

if st.session_state['frequency'] == 'Daily':
    selected_day = st.segmented_control(
        "Which days should this run each week?",
        options=daily_map.keys(),
        format_func=lambda option: daily_map[option],
        selection_mode="multi"
    )
    selected_time = st.time_input("Time", datetime.time(8, 45))

if st.session_state['frequency'] == 'Monthly':
    selected_month = st.slider("Which day of the month should this run?", 1,31,1)
    selected_time = st.time_input("Time", datetime.time(8, 45))

if st.session_state['frequency'] == 'Weekly':
    selected_day = st.segmented_control(
        "Which day of the week should this run?",
        options=daily_map.keys(),
        format_func=lambda option: daily_map[option],
        selection_mode="single"
    )
    selected_time = st.time_input("Time", datetime.time(8, 45))
    
if st.button('Create Results Email Job'):
    query_object = w.queries.get(selected_query_id)

    cron_statement = "0 {0} {1}".format(selected_time.minute, selected_time.hour)
    if selected_frequency == "Monthly":
        cron_statement = cron_statement + " " + str(selected_month) + " * ?"
    elif selected_frequency == 'Daily':
        cron_statement = cron_statement + " ? * " +','.join(selected_day) + " *"
    elif selected_frequency == "Weekly":
        cron_statement = cron_statement + " ? * " + selected_day + " *"
    st.session_state.clear()

    new_job = JobSettings.from_dict(
        {
            "name": "User Email Job {0} - {1}".format(current_user.display_name, query_object.display_name),
            "schedule": {
                "quartz_cron_expression": "{0}".format(cron_statement),
                "timezone_id": "UTC",
            },
            "tasks": [
                {
                    "task_key": "Run_Notebook",
                    "notebook_task": {
                        "notebook_path": "{0}".format(os.environ.get("EMAIL_NOTEBOOK_PATH")),
                        "base_parameters": {
                            "query_id": "{0}".format(selected_query_id),
                            "email_recipients": "{0}".format(email_to),
                            "from_mailbox": "{0}".format(os.environ.get("MAIL_FROM")),
                        },
                        "source": "WORKSPACE",
                    },
                    "job_cluster_key": "Job_cluster",
                },
            ],
            "job_clusters": [
                {
                    "job_cluster_key": "Job_cluster",
                    "new_cluster": {
                        "spark_version": "16.4.x-scala2.12",
                        "azure_attributes": {
                            "first_on_demand": 1,
                            "spot_bid_max_price": -1,
                        },
                        "node_type_id": "Standard_D4ds_v5",
                        "spark_env_vars": {
                            "PYSPARK_PYTHON": "/databricks/python3/bin/python3",
                        },
                        "data_security_mode": "DATA_SECURITY_MODE_DEDICATED",
                        "runtime_engine": "PHOTON",
                        "kind": "CLASSIC_PREVIEW",
                        "is_single_node": True,
                    },
                },
            ],
            "queue": {
                "enabled": True,
            },
            #"run_as": {
            #    "user_name": "{0}".format(current_user.user_name)
            #}
        }
    )
    try:
        #create_job_client = WorkspaceClient()
        result = w.jobs.create(**new_job.as_shallow_dict())
        st.success("Success! Job created. View job details here: http://{0}/jobs/{1}".format(workspace_url, result.job_id))
    except Exception as e:
        st.error("Error creating job: {0}".format(e))