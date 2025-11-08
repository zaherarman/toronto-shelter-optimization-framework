"""
Two objectives:
    1. Primary: shelter as many homeless as possible (i.e., minimising unsheltered z_hg)
    2. Secondary: Among all the ways that achieve that best coverage, choose the one with
                  the shortest total walking distance
"""

import gurobipy as gp
from gurobipy import Model, GRB, quicksum


def build_model(
    H,  # Set for hotspots #! ONE HOTSPOT
    S,  # Set for shelters
    G,  # Set for genders
    d,  # Parameter dict: {(h,g): arrival rate} #! REMOVE
    c,  # Parameter dict: {s: capacity}
    e,  # Parameter dict: {(s,g): eligibility (0/1)}
    w,  # Parameter dict: {(h,s): walking time}
    type="integer",  # "integer", "linear", "mixed"
    time_limit=None,
    mip_gap=None,
):

    # Feasible hotspot shelter pairs fot safety
    A = {(h,s) for h in H for s in S if (h,s) in w}

    x_vtype = GRB.INTEGER if type in {"integer", "mixed"}:
    z_vtype = GRB.INTEGER if type == "integer" else GRB.CONTINUOUS
    
    m = Model("shelter_optimization")
    
    # Decision Variables
    x = {}
    for h, s in A:
        for g in G:
            x[(h,s,g)] = m.addVar(vtype=x_vtype, lb=0.0, name=f"x[{h},{s},{g}]")
            
    z = {}
    for h in H:
        for g in G:
            z[(h,g)] = m.addVar(vtype=z_vtype, lb=0.0, name=f"z[{h},{g}]")
            
            
            
            
            
            

    