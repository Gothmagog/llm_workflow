import re
import logging
from typing import Optional, List, Union
from functools import lru_cache
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field
from datacommons_client.client import DataCommonsClient

apikey_datacommons = "W8KGlv2iBArravlHGkhBRGFD2uI8dauSgY0pFcQq7dlo85EM"
client = DataCommonsClient(api_key=apikey_datacommons)
log = logging.getLogger("main")

# List Provenances
@lru_cache
def _list_provenances():
    # Get DCIDs of all the provenances
    resp = client.node.fetch_property_values(node_dcids=["Provenance"], properties="typeOf", out=False).to_dict()
    nodes = resp["data"]["Provenance"]["arcs"]["typeOf"]["nodes"]
    dcids = [n["dcid"] for n in nodes]
    data = {n["dcid"]: {"name": n["name"]} for n in nodes}
    
    # Get the associated URL and latest update date for each
    resp = client.node.fetch_property_values(node_dcids=dcids, properties=["url", "latestObservationDate"]).to_dict()
    for obj in resp["data"].values():
        arcs = obj["arcs"]
        if "url" in arcs:
            for n in arcs["url"]["nodes"]:
                data[n["provenanceId"]]["url"] = n["value"]
        if "latestObservationDate" in arcs:
            for n in arcs["latestObservationDate"]["nodes"]:
                data[n["provenanceId"]]["latestObservationDate"] = n["value"]

    # Split into content and artifacts
    content = [{"name": v["name"], "latestObservationDate": "latestObservationDate" in v and v["latestObservationDate"]} for v in data.values()]
    artifacts = list(set([v["url"] for v in data.values()]))

    return content, artifacts

list_provenances_tool = StructuredTool.from_function(
    func=_list_provenances,
    name="ListDataProvenances",
    description="Lists all available data provenances",
    response_format="content_and_artifact",
    return_direct=False)

# List Sources
@lru_cache
def _list_sources():
    resp = client.node.fetch_property_values(node_dcids=["Source"], properties="typeOf", out=False).to_dict()
    names = [n["name"] for n in resp["data"]["Source"]["arcs"]["typeOf"]["nodes"]]
    dcids = [n["dcid"] for n in resp["data"]["Source"]["arcs"]["typeOf"]["nodes"]]
    return names, dcids

list_sources_tool = StructuredTool.from_function(
    func=_list_sources,
    name="ListDataProviders",
    description="Lists all available data providers",
    response_format="content_and_artifact",
    return_direct=False)

# List Datasets in Source
class ListDatasetsInput(BaseModel):
    source_dcid: str = Field(name="DataProviderID", description="The ID of the data provider")

@lru_cache(maxsize=10)
def _list_datasets(source_dcid):
    # Pull datasets that are part of the given source
    resp = client.node.fetch_property_values(node_dcids=[source_dcid], properties="isPartOf", out=False).to_dict()
    print(resp)
    names = [n["name"] for n in resp["data"][source_dcid]["arcs"]["isPartOf"]["nodes"]]
    dcids = [n["dcid"] for n in resp["data"][source_dcid]["arcs"]["isPartOf"]["nodes"]]
    return names, dcids

list_datasets_tool = StructuredTool.from_function(
    func=_list_datasets,
    name="ListsSourceDatasets",
    description="Lists the available datasets from a given data provider",
    args_schema=ListDatasetsInput,
    response_format="content_and_artifact",
    return_direct=False)

# Explore Stat Var Groups
class ExploreStatVarGroupsInput(BaseModel):
    parent_group: Optional[str] = Field(name="ParentGroup", default=None, description="The ID of the parent variable group whose child groups you wish to retrieve")
class ExploreStatVarGroupsOutput(BaseModel):
    name: str = Field(description="The group's human readable name")
    id: str = Field(description="The ID of the group")
    
def _explore_stat_var_groups(parent_group=None) -> List[ExploreStatVarGroupsOutput]:
    log.debug(f"_explore_stat_var_groups: '{parent_group}'")
    dcid = parent_group
    if not parent_group:
        dcid = "dc/g/Root"
    resp = client.node.fetch(node_dcids=[dcid], expression="<-specializationOf").to_dict()["data"]
    if dcid in resp and "arcs" in resp[dcid]:
        nodes = resp[dcid]["arcs"]["specializationOf"]["nodes"]
        return [{"name": n["name"], "id": n["dcid"]} for n in nodes]

    return None

explore_stat_var_groups_tool = StructuredTool.from_function(
    func=_explore_stat_var_groups,
    name="ExploreStatisticalVariableGroups",
    description="Statistical variables are grouped together in statistical variable groups. This function drills down into the tree hierarchy of statistical variable groups, and returns all variable groups beneath a given parent. Passing in no parent returns the top-tier variable groups.",
    args_schema=ExploreStatVarGroupsInput,
    return_direct=False)

@lru_cache(maxsize=10)
def _get_county_dcid(state_name, county_name):
    # Get the state
    state = client.resolve.fetch(node_ids=[state_name], expression="<-description{typeOf:State}->dcid").to_flat_dict()[state_name]

    # Get the target county in State
    counties = client.node.fetch_place_children(state, children_type="County")[state]
    county = None
    for c in counties:
        if county_name.lower() in c["name"].lower():
            county = c["dcid"]
            break
    if not county:
        raise Exception(f"Couldn't find county {target_county}")

    return county

@lru_cache(maxsize=3)
def _get_vars_for_county(county_id):
    log.debug(f"  getting variables for county {county_id}...")
    all_variables = client.observation.fetch_available_statistical_variables(entity_dcids=[county_id])[county_id]
    log.debug("  getting stat var groups that the variables belong to...")
    resp = None
    ret = {}
    fetch_count = 0
    fetch_incr = 2000
    page_count = 0
    variables = []
    while True:
        if not resp or (resp and not resp.nextToken):
            if fetch_count * fetch_incr >= len(all_variables):
                break
            fetch_count += 1
            page_count = 1
            variables = all_variables[(fetch_count - 1) * fetch_incr: fetch_count * fetch_incr]
        else:
            page_count += 1
        log.debug(f"    fetch # {fetch_count}, page {page_count}")
        if page_count > 1:
            resp = client.node.fetch(node_dcids=variables, expression="->memberOf", all_pages=False, next_token=resp.nextToken)
        else:
            resp = client.node.fetch(node_dcids=variables, expression="->memberOf", all_pages=False)
        variable_stat_var_groups = resp.to_dict()["data"]
        for v in variable_stat_var_groups.keys():
            if "arcs" in variable_stat_var_groups[v]:
                node = variable_stat_var_groups[v]["arcs"]["memberOf"]["nodes"][0]
                ret[v] = { "group_id": node["dcid"] }
    return ret

# List statistical variables available for a county
class ListVariablesInput(BaseModel):
    state_name: str = Field(name="StateName", description="The state the county belongs in; only use the state's name and no other words")
    county_name: str = Field(name="CountyName", description="The county we're pulling staticical variables for; only use the county's name and no other words")
    stat_var_group: str = Field(name="StatisticalVariableGroupID", description="The ID of the statistical variable group that all the county's variables must belong to")

def _list_variables_for_state_and_county(state_name, county_name, stat_var_group) -> List[str]:
    log.debug(f"_list_variables_for_state_and_county: '{state_name}', '{county_name}', {stat_var_group}")
    county = _get_county_dcid(state_name, county_name)
    
    # Get a list of variables for that state
    variables = _get_vars_for_county(county)
    
    ret = [k for k, v in variables.items() if v["group_id"] == stat_var_group]
    if len(ret) == 0:
        log.debug("Returning no results")
        return None
    log.debug(f"Returning {len(ret)} results")
    return ret

list_variables_tool = StructuredTool.from_function(
    func=_list_variables_for_state_and_county,
    name="ListsVariablesForCounty",
    description="Lists the available statictical variables for a given county and statistical variable group. If an empty resultset is returned, try drilling further down into the statistical variable group, or try a different one.\n\nThe group hierarchy can be quite deep, sometimes, 7-8 levels deep, so be persistent.",
    args_schema=ListVariablesInput,
    return_direct=False)

# Get statistical daqta points for a county
class GetObservationsForStateAndCountyInput(BaseModel):
    variable_ids: List[str] = Field(name="VariableIDs", description="A list of variable IDs identifying the specific data points (or 'observations') to gather")
    state_name: str = Field(name="StateName", description="The state the county belongs in; only use the state's name and no other words")
    county_name: str = Field(name="CountyName", description="The county we're pulling data for; only use the county's name and no other words")
class ObservationOutput(BaseModel):
    date: str = Field(description="The date this particular observation applies to. This can be a single year, a month, or any other kind of date range.")
    value: Union[int, float] = Field(description="The value of this observation. This will always be a number, either an integer or floating point.")
    data_source: str = Field(name="DataSource", description="Identifier for the source of this particular observation, usually an organization-specific dataset")
    
def _get_observations_for_state_and_county(variable_ids, state_name, county_name) -> dict[str, ObservationOutput]:
    log.debug(f"_get_observations_for_state_and_county: {variable_ids}, '{state_name}', '{county_name}'")
    county_id = _get_county_dcid(state_name, county_name)
    resp = client.observation.fetch(variable_dcids=variable_ids, entity_dcids=[county_id]).to_dict()
    ret = {}
    facets = {}

    if not "byVariable" in resp:
        raise Exception(f"Unexpected format in API response: {resp}")

    if "facets" in resp:
        for k, v in resp["facets"].items():
            facets[k] = v["importName"]
    
    for var_id, entity_observation in resp["byVariable"].items():
        ret[var_id] = []
        if "byEntity" in entity_observation:
            facet_observations = entity_observation["byEntity"][county_id]["orderedFacets"]
            for facet_observation in facet_observations:
                cur_facet_id = facet_observation["facetId"]
                ret[var_id] += [{"date": fo["date"], "value": fo["value"], "data_source": facets[cur_facet_id]} for fo in facet_observation["observations"]]
        else:
            log.warn("byEntity not found in variable observation")
    return ret

get_county_observations_tool = StructuredTool.from_function(
    func=_get_observations_for_state_and_county,
    name="GetDataPointsForCounty",
    description="Retrieve data points for a specific county. The actual data retrieved depends on the variable IDs passed in; these should be retrieved from the 'ListsVariablesForCounty' function. The returned data is a list of objects that includes: the value of the data point, the applicable date of that data, and the identifier for the data source that sourced the data.\n\nIf an empty dataset is returned, you need more specific statistical variable groups; drill down further in the ListsVariablesForCounty to find them.",
    args_schema=GetObservationsForStateAndCountyInput,
    return_direct=False)
