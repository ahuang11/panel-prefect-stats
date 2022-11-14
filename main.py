# import time
import sys

import pandas as pd
import panel as pn
import requests
from bokeh.models.widgets.tables import HTMLTemplateFormatter

# from datetime import timedelta


# from prefect import flow, task
# from prefect.tasks import task_input_hash

CSS = """
    body {
        margin-top: 1%;
        margin-bottom: 1%;
        margin-left: 20%;
        margin-right: 20%;
    }

    .bk.bk-btn.bk-btn-default {
        font-size: 105%;
    }
"""
MAX_WIDTH = 1000
DAY_IN_SECONDS = 3600 * 24
CACHE_KWARGS = dict(ttl=DAY_IN_SECONDS, policy="FIFO")
COLUMNS = ["📕 Org Repo", "⭐ Stars", "⬇ Downloads", "👀 Watching"]
if sys.platform != "emscripten":
    CACHE_KWARGS["to_disk"] = True

pn.extension("tabulator", sizing_mode="stretch_width", notifications=True)
pn.config.raw_css.append(CSS)


# @task(cache_key_fn=task_input_hash, retries=3, cache_expiration=timedelta(days=1))
@pn.cache(**CACHE_KWARGS)
def parse_catalog():
    # catalog_resp = requests.get("https://docs.prefect.io/collections/catalog/")
    # repo_api_urls = sorted({
    #     url.split('"')[1]
    #     .replace("https://", "https://api.github.com/repos/")
    #     .replace(".github.io", "")
    #     .rstrip("/")
    #     for url in catalog_resp.text.split("<a href=")
    #     if ".io/prefect-" in url
    # })
    repo_api_urls = ['https://api.github.com/repos/alessandrolollo/prefect-cubejs', 'https://api.github.com/repos/alessandrolollo/prefect-metricflow', 'https://api.github.com/repos/alessandrolollo/prefect-stitch', 'https://api.github.com/repos/alessandrolollo/prefect-transform', 'https://api.github.com/repos/fivetran/prefect-fivetran', 'https://api.github.com/repos/fugue-project/prefect-fugue', 'https://api.github.com/repos/khuyentran1401/prefect-alert', 'https://api.github.com/repos/madkinsz/prefect-kv', 'https://api.github.com/repos/prefecthq/prefect-airbyte', 'https://api.github.com/repos/prefecthq/prefect-aws', 'https://api.github.com/repos/prefecthq/prefect-azure', 'https://api.github.com/repos/prefecthq/prefect-census', 'https://api.github.com/repos/prefecthq/prefect-dask', 'https://api.github.com/repos/prefecthq/prefect-databricks', 'https://api.github.com/repos/prefecthq/prefect-dbt', 'https://api.github.com/repos/prefecthq/prefect-docker', 'https://api.github.com/repos/prefecthq/prefect-email', 'https://api.github.com/repos/prefecthq/prefect-firebolt', 'https://api.github.com/repos/prefecthq/prefect-gcp', 'https://api.github.com/repos/prefecthq/prefect-github', 'https://api.github.com/repos/prefecthq/prefect-gitlab', 'https://api.github.com/repos/prefecthq/prefect-great-expectations', 'https://api.github.com/repos/prefecthq/prefect-hex', 'https://api.github.com/repos/prefecthq/prefect-hightouch', 'https://api.github.com/repos/prefecthq/prefect-jupyter', 'https://api.github.com/repos/prefecthq/prefect-monday', 'https://api.github.com/repos/prefecthq/prefect-monte-carlo', 'https://api.github.com/repos/prefecthq/prefect-openmetadata', 'https://api.github.com/repos/prefecthq/prefect-ray', 'https://api.github.com/repos/prefecthq/prefect-shell', 'https://api.github.com/repos/prefecthq/prefect-slack', 'https://api.github.com/repos/prefecthq/prefect-snowflake', 'https://api.github.com/repos/prefecthq/prefect-sqlalchemy', 'https://api.github.com/repos/prefecthq/prefect-twitter', 'https://api.github.com/repos/sodadata/prefect-soda-core']  # noqa
    return repo_api_urls


# @task(cache_key_fn=task_input_hash, retries=3, cache_expiration=timedelta(days=1))
@pn.cache(**CACHE_KWARGS)
def get_stats(repo_api_url):
    print(repo_api_url)
    repo_api_data = requests.get(repo_api_url).json()
    repo_name = repo_api_data["name"]
    repo_full_name = repo_api_data["full_name"]
    repo_stars = repo_api_data["stargazers_count"]
    repo_subscribers = repo_api_data["subscribers_count"]
    # repo_downloads = requests.get(
    #     f"https://pypistats.org/api/packages/{repo_name}/recent?period=month"
    # ).json()["data"]["last_month"]
    repo_downloads = None
    repo_url = repo_api_data["html_url"]
    repo_df = pd.DataFrame(
        {
            "org repo": [f'<a href="{repo_url}" target="_blank">{repo_full_name}</a>'],
            "stars": [repo_stars],
            "downloads": [repo_downloads],
            "subscribers": [repo_subscribers],
        },
        index=[repo_full_name],
    )
    repo_df.columns = COLUMNS
    return repo_df


@pn.cache(**CACHE_KWARGS)
def get_repo_dfs():
    repo_api_urls = parse_catalog()

    repo_dfs = []
    for repo_api_url in repo_api_urls:
        repo_df = get_stats(repo_api_url)
        repo_dfs.append(repo_df)
    return repo_dfs


@pn.cache(**CACHE_KWARGS)
def update_table(repo_dfs):
    base_df = tabulator.value
    all_df = pd.concat([base_df.iloc[[0]], *repo_dfs]).sort_values(
        COLUMNS[1:], ascending=False
    )
    tabulator.value = all_df


# @flow(persist_result=True)
@pn.cache(**CACHE_KWARGS)
def load_data():
    repo_dfs = get_repo_dfs()
    update_table(repo_dfs)


@pn.cache(**CACHE_KWARGS)
def get_star_plot(selected_repos, type_):
    api_url = (
        f"https://api.star-history.com/svg?repos={selected_repos}&type={type_}"  # noqa
    )
    return requests.get(api_url).text.replace(
        """<svg width="800" xmlns="http://www.w3.org/2000/svg" style="stroke-width: 3; font-family: xkcd; background: white;" height="533.3333333333334" preserveaspectratio="xMidYMid meet">""",  # noqa
        """<svg xmlns="http://www.w3.org/2000/svg" style="stroke-width: 3; font-family: xkcd; background: white;height:100%;width:100%" viewBox="0 0 800 533" preserveAspectRatio="xMidYMid meet">""",  # noqa
    )


def get_sum(repo_df, selected_index):
    if not selected_index:
        selected_index = [0]
    selected_df = repo_df.iloc[selected_index]

    numbers = pn.Row(
        pn.Spacer(),
        *[
            pn.indicators.Number(
                name="",
                value=selected_df[column].sum(),
                font_size="28pt",
                format=column[0] + " {value:,d} total",
                align="center",
                sizing_mode="stretch_width",
            )
            for column in COLUMNS[1:]
        ],
        pn.Spacer(),
        align="center",
    )
    return numbers


@pn.cache(**CACHE_KWARGS)
def get_star_history(repo_df, selected_index, by_date):
    if not selected_index:
        selected_index = [0]
    elif len(selected_index) > 1:
        if 0 in selected_index:
            selected_index.remove(0)

    selected_repos = ",".join(repo_df.iloc[selected_index].index)
    type_ = "Date" if by_date else "Timeline"
    svg_text = get_star_plot(selected_repos, type_)
    svg = pn.pane.SVG(
        svg_text,
        min_width=1000,
        min_height=1000,
        sizing_mode="scale_both",
    )
    return svg


# @flow
def initialize_widgets():
    repo_df = get_stats("https://api.github.com/repos/prefecthq/prefect")
    tabulator = pn.widgets.Tabulator(
        repo_df,
        selection=[0],
        show_index=False,
        max_width=MAX_WIDTH,
        align="center",
        theme="modern",
        layout="fit_columns",
        formatters={column: HTMLTemplateFormatter() for column in COLUMNS},
        disabled=True,
    )

    download_column = pn.Row(
        *tabulator.download_menu(
            text_kwargs={"name": "📁 Enter filename", "value": "prefect_stats.csv"},
            button_kwargs={"name": "⬇️ Download table"},
        )
    )
    download_column[1].align = "end"

    toggle = pn.widgets.Toggle(
        name="📅 Align by date", max_width=MAX_WIDTH, align="center"
    )

    numbers = pn.bind(
        get_sum,
        repo_df=tabulator.param.value,
        selected_index=tabulator.param.selection,
    )

    svg = pn.bind(
        get_star_history,
        repo_df=tabulator.param.value,
        selected_index=tabulator.param.selection,
        by_date=toggle.param.value,
    )
    return tabulator, download_column, toggle, numbers, svg

tabulator, download_column, toggle, numbers, svg = initialize_widgets()
sidebar_column = pn.Column(
    pn.WidgetBox(
        """
    # 👋 Welcome!

    The app is fetching stats from all publicly available Prefect repositories listed on the
    Prefect Collections Catalog.

    Upon completion, select desired rows (shift + click) to view the
    corresponding repos' star histories. To visit the GitHub repo,
    click on the name.
    """
    ),
    tabulator,
    download_column,
    toggle,
    sizing_mode="stretch_both",
)

main_column = pn.Column(
    numbers,
    svg,
    sizing_mode="stretch_both",
)

dashboard = pn.template.FastListTemplate(
    title="Prefect GitHub Numbers",
    header_background="#0052FF",
    sidebar_width=MAX_WIDTH,
    sidebar=sidebar_column,
    main=main_column,
    logo="https://github.com/PrefectHQ/prefect/blob/main/docs/img/logos/prefect-logo-mark-solid-white-500.png?raw=true",  # noqa
    favicon="https://www.prefect.io/assets/static/favicon.ce0531f.c41309e9925f6ce1d5a1ff078f9a7f0b.png",
)
pn.state.onload(load_data)
# periodic_updates = pn.state.add_periodic_callback(load_data, period=5000)
dashboard.servable()
