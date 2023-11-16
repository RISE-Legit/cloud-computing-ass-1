from flask import Flask, render_template, jsonify
from google.cloud import bigquery
from google.cloud import logging as gcloud_logging
import logging

project = "s3814655-oua23sp3-task-2"
dataset = "import_export"
# App URL: https://s3814655-oua23sp3-task-2.ts.r.appspot.com
# Service account: s3814655-oua23sp3-task-2@appspot.gserviceaccount.com
app = Flask(__name__)
client = bigquery.Client()
log_client = gcloud_logging.Client()
log_client.get_default_handler()
log_client.setup_logging()

# Grabs the key from $env:GOOGLE_APPLICATION_CREDENTIALS="path/to/key.json".
client = bigquery.Client()


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/query1")
def run_query1():
    try:
        job = client.query(query1)
        results = job.result()
        if not results:
            return jsonify({"error": "No results returned from the query."})
        field_names = [field.name for field in results.schema]
        rows_as_dicts = [dict(zip(field_names, row)) for row in results]
        return jsonify(rows_as_dicts)
    except Exception as e:
        return jsonify({"JSON error": str(e)})


@app.route("/query2")
def run_query2():
    try:
        job = client.query(query2)
        results = job.result()
        if not results:
            return jsonify({"error": "No results returned from the query."})
        field_names = [field.name for field in results.schema]
        rows_as_dicts = [dict(zip(field_names, row)) for row in results]

        # Change "trade_deficit_value" to "trade deficit value"
        for row_dict in rows_as_dicts:
            if "trade_deficit_value" in row_dict:
                row_dict["trade deficit value"] = row_dict["trade_deficit_value"]
                del row_dict["trade_deficit_value"]
        return jsonify(rows_as_dicts)
    except Exception as e:
        return jsonify({"JSON error": str(e)})


@app.route("/query3")
def run_query3():
    try:
        # Query1.
        results1 = client.query(query1).result()
        if not results1:
            return jsonify({"error": "No results1 returned from query1."})
        top_times = [row["time_ref"] for row in results1]
    except Exception as e:
        return jsonify({"error": "Query1 failed."})
    try:
        # Query2.
        results2 = client.query(query2).result()
        if not results2:
            return jsonify({"error": "No results2 returned from query2."})
        top_countries = [row["country_label"] for row in results2]
    except Exception as e:
        return jsonify({"error": "Query1 failed."})
    try:
        # Combined query.
        query3 = construct_query3(top_times, top_countries)
        results3 = client.query(query3).result()
        if not results3:
            return jsonify({"error": "No results3 returned from query3."})
        field_names = [field.name for field in results3.schema]
        rows_as_dicts = [dict(zip(field_names, row)) for row in results3]
        return jsonify(rows_as_dicts)
    except Exception as e:
        return jsonify({"JSON error": str(e)})


query1 = f"""
        SELECT 
            LEFT(CAST(time_ref AS STRING), 4) AS Year,
            RIGHT(CAST(time_ref AS STRING), 2) AS Month,  
            time_ref,
            SUM(value) AS Trade_Value
        FROM 
            `{project}.{dataset}.gsquarterlySeptember20`
        WHERE 
            account IN ('Imports', 'Exports')
        GROUP BY 
            time_ref
        ORDER BY 
            Trade_Value DESC
        LIMIT 10;
        """

query2 = f"""
        WITH TradeDeficit AS (
        SELECT 
            cc.country_label,
            gs.product_type,
            (SUM(CASE WHEN gs.account = 'Imports' THEN gs.value ELSE 0 END) -
             SUM(CASE WHEN gs.account = 'Exports' THEN gs.value ELSE 0 END)) AS trade_deficit_value,
            gs.status
        FROM 
            `{project}.{dataset}.gsquarterlySeptember20` gs
        JOIN 
            `{project}.{dataset}.country_classification` cc ON gs.country_code = cc.country_code
        WHERE 
            gs.status = 'F'
            AND gs.product_type = 'Goods'
            AND (CAST(LEFT(CAST(gs.time_ref AS STRING), 4) AS INT64) BETWEEN 2013 AND 2015)
        GROUP BY 
            cc.country_label, gs.product_type, gs.status
        )
        SELECT 
            country_label,
            product_type,
            trade_deficit_value,
            status
        FROM 
            TradeDeficit
        ORDER BY 
            trade_deficit_value DESC
        LIMIT 40;"""


def construct_query3(top_times, top_countries):
    quoted_times = ", ".join(str(time) for time in top_times)
    escaped_countries = [country.replace("'", "\\'") for country in top_countries]
    quoted_countries = ", ".join(f"'{country}'" for country in escaped_countries)
    app.logger.warning(f"Quoted_time: {quoted_times}")
    app.logger.warning(f"quoted_countries: {quoted_countries}")
    query3 = f"""
    SELECT
        sc.service_label,
        (SUM(CASE WHEN gs.account = 'Exports' THEN gs.value ELSE 0 END) -
        SUM(CASE WHEN gs.account = 'Imports' THEN gs.value ELSE 0 END)) AS trade_surplus_value
    FROM
        {project}.{dataset}.services_classification sc
    JOIN
        {project}.{dataset}.gsquarterlySeptember20 gs ON gs.code = sc.code
    JOIN
        {project}.{dataset}.country_classification cc ON cc.country_code = gs.country_code
    WHERE
        gs.time_ref IN ({quoted_times})
    AND
        cc.country_label IN ({quoted_countries})
    AND
        gs.product_type = 'Services'
    GROUP BY
        sc.service_label
    ORDER BY
        trade_surplus_value DESC
    LIMIT 25;
    """
    app.logger.warning(f"Query3: {query3}")
    return query3


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True)
