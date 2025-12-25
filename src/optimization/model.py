from __future__ import annotations
from typing import Any, Dict, List, Tuple
from gurobipy import Model, GRB, quicksum


def build_model_multi_hotspot(
    hotspots: List[Any],
    shelters: List[Any],
    genders: List[Any],
    demand: Dict[Tuple[Any, Any], float],
    capacities: Dict[Any, float],
    dist: Dict[Tuple[Any, Any], float],
    eligibility: Dict[Tuple[Any, Any], int],
):
    """
    Two-stage optimization:

    Stage 1: Minimize total unsheltered (sum z)
    Stage 2: Fix unsheltered PER GENDER at Stage 1 optimum, then minimize distance
    """
    
    H = hotspots
    S = shelters
    G = genders

    m = Model("shelter_referral")

    # Decision variables
    x = {(h, s, g): m.addVar(vtype=GRB.CONTINUOUS, lb=0, name=f"x_{h}_{s}_{g}") for h in H for s in S for g in G}
    z = {(h, g): m.addVar(vtype=GRB.CONTINUOUS, lb=0, name=f"z_{h}_{g}") for h in H for g in G}

    m.update()

    # Demand conservation: sum_s x[h,s,g] + z[h,g] == demand[h,g]
    for h in H:
        for g in G:
            m.addConstr(
                quicksum(x[(h, s, g)] for s in S) + z[(h, g)] == demand[(h, g)],
                name=f"demand_{h}_{g}",
            )

    # Capacity: sum_{h,g} x[h,s,g] <= cap[s]
    for s in S:
        m.addConstr(
            quicksum(x[(h, s, g)] for h in H for g in G) <= capacities[s],
            name=f"cap_{s}",
        )

    # Eligibility: if not eligible, force x=0
    for s in S:
        for g in G:
            if eligibility.get((s, g), 0) == 0:
                for h in H:
                    m.addConstr(x[(h, s, g)] == 0, name=f"elig_{s}_{g}_{h}")

    # Stage 1: minimize unsheltered
    m.setObjective(quicksum(z[(h, g)] for h in H for g in G), GRB.MINIMIZE)
    m.optimize()

    Z_star_per_gender = {g: sum(z[(h, g)].X for h in H) for g in G}
    Z_star_total = sum(Z_star_per_gender.values())

    # Stage 2: fix unsheltered per gender, minimize distance
    for g in G:
        m.addConstr(
            quicksum(z[(h, g)] for h in H) == Z_star_per_gender[g],
            name=f"fix_unsheltered_{g}",
        )

    m.setObjective(
        quicksum(dist[(h, s)] * x[(h, s, g)] for h in H for s in S for g in G),
        GRB.MINIMIZE,
    )
    m.optimize()

    return m, {"x": x, "z": z}, {"Z_star": Z_star_total, "obj_distance": m.ObjVal}
