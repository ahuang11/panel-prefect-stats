import time
import httpx
from bs4 import BeautifulSoup
from datetime import timedelta

import panel as pn
import pandas as pd

from prefect import flow, task
from prefect.tasks import task_input_hash

CSS = """
    body {
        margin-top: 1%;
        margin-bottom: 1%;
        margin-left: 20%;
        margin-right: 20%;
    }
"""
MAX_WIDTH = 1500
pn.extension("tabulator", sizing_mode="stretch_width")
pn.config.raw_css.append(CSS)


@task(cache_key_fn=task_input_hash, retries=3, cache_expiration=timedelta(days=1))
def parse_catalog():
    if "catalog" not in pn.state.cache:
        catalog_resp = httpx.get("https://docs.prefect.io/collections/catalog/")
        catalog_soup = BeautifulSoup(catalog_resp.text, "html.parser")
        repo_api_urls = {
            url["href"]
            .replace("https://", "https://api.github.com/repos/")
            .replace(".github.io", "")
            .rstrip("/")
            for url in catalog_soup.find_all("a", href=True)
            if ".io/prefect-" in url["href"]
        }
        pn.state.cache["catalog"] = repo_api_urls
    else:
        repo_api_urls = pn.state.cache["catalog"]
    return repo_api_urls


@task(cache_key_fn=task_input_hash, retries=3, cache_expiration=timedelta(days=1))
def get_stats(repo_api_url):
    repo_api_data = httpx.get(repo_api_url).json()
    repo_name = repo_api_data["name"]
    repo_full_name = repo_api_data["full_name"]
    repo_stars = repo_api_data["stargazers_count"]
    repo_subscribers = repo_api_data["subscribers_count"]
    repo_downloads = httpx.get(
        f"https://pypistats.org/api/packages/{repo_name}/recent?period=month"
    ).json()["data"]["last_month"]
    repo_df = pd.DataFrame(
        {
            "org repo": [repo_full_name],
            "stars": [repo_stars],
            "downloads": [repo_downloads],
            "subscribers": [repo_subscribers],
        }
    )
    repo_df.columns = repo_df.columns.str.title()
    return repo_df


def update_table(repo_dfs):
    base_df = tabulator.value
    all_df = pd.concat([base_df.iloc[[0]], *repo_dfs]).sort_values(
        ["Stars", "Downloads", "Subscribers"], ascending=False
    )
    all_df.index = list(range(len(all_df)))
    tabulator.value = all_df
    tabulator.loading = False
    toggle.disabled = False


@flow(persist_result=True)
def load_data():
    if "data" not in pn.state.cache:
        repo_api_urls = parse_catalog()
        repo_dfs = []
        for repo_api_url in repo_api_urls:
            repo_name = repo_api_url.split("/")[-1]
            repo_df = get_stats.with_options(name=repo_name)(repo_api_url)
            repo_dfs.append(repo_df)
        pn.state.cache["data"] = repo_dfs
    else:
        repo_dfs = pn.state.cache["data"]
    update_table(repo_dfs)


def get_star_history(repo_df, selected_index, by_date):
    if not selected_index:
        selected_index = [0]
    elif len(selected_index) > 1:
        if 0 in selected_index:
            selected_index.remove(0)

    type_ = "Date" if by_date else "Timeline"
    selected_repos = ",".join(repo_df.iloc[selected_index]["Org Repo"])
    api_url = f"https://api.star-history.com/svg?repos={selected_repos}&type={type_}"  # noqa
    svg_text = httpx.get(api_url).text
    svg = pn.pane.SVG(
        svg_text,
        min_width=1000,
        min_height=1000,
        sizing_mode="scale_both",
        link_url=api_url
    )
    return svg


@flow
def initialize_widgets():
    static_text = pn.widgets.StaticText(
        name="Instructions",
        value="""
            Welcome! The app is fetching stats from all publicly
            available Prefect repositories. Please be patient
            while it loads as this app is not yet optimized.
            Upon completion, select desired rows (shift + click) to view the
            corresponding repos' star histories.
        """,
    )
    toggle = pn.widgets.Toggle(
        name="By Date 📅", max_width=MAX_WIDTH, align="center", disabled=True
    )

    repo_df = get_stats.fn("https://api.github.com/repos/prefecthq/prefect")
    tabulator = pn.widgets.Tabulator(
        repo_df,
        selection=[0],
        show_index=False,
        max_width=MAX_WIDTH,
        align="center",
        theme="modern",
        layout="fit_columns",
        disabled=True,
        loading=True,
    )

    svg = pn.bind(
        get_star_history,
        repo_df=tabulator.param.value,
        selected_index=tabulator.param.selection,
        by_date=toggle.param.value,
    )
    return static_text, tabulator, toggle, svg


static_text, tabulator, toggle, svg = initialize_widgets()
sidebar_column = pn.Column(static_text, tabulator, toggle, sizing_mode="stretch_both")
dashboard = pn.template.FastGridTemplate(
    title="Prefect Stats",
    header_background="#0052FF",
    sidebar_width=700,
    sidebar=sidebar_column,
    logo="https://github.com/PrefectHQ/prefect/blob/main/docs/img/logos/prefect-logo-mark-solid-white-500.png?raw=true",  # noqa
)
dashboard.main[0:8, :] = svg
pn.state.onload(load_data)
dashboard.servable()
