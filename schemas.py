# schemas.py
from pydantic import BaseModel, Field
from typing import List, Optional

class AssociationRuleSchema(BaseModel):
    """Matches raw output vectors from the FP-Growth execution payload"""
    antecedents: List[str] = Field(..., description="The triggering items bought by consumer")
    consequents: List[str] = Field(..., description="The highly correlated target items")
    support: float = Field(..., description="Overall population frequency")
    confidence: float = Field(..., description="Predictive certainty parameter")
    lift: float = Field(..., description="Statistical correlation strength factor")

class MerchandisingAction(BaseModel):
    """Enforces structurally sound outputs from the agent network"""
    strategy_type: str = Field(..., description="E.g., Adjacent Shelf, Cross-Contextual Signage, Endcap")
    target_location: str = Field(..., description="Specific target zone or aisle in retail space")
    compliance_status: str = Field(..., description="Marked APPROVED or COMPLIANCE_MODIFIED")
    actionable_copy: Optional[str] = Field(None, description="Text-only promotional messaging for signs")
    justification: str = Field(..., description="Psychological or mathematical rationale for the layout placement")

class FinalLayoutBlueprint(BaseModel):
    """The master JSON payload dispatched down the WebSockets pipe"""
    job_id: str
    status: str
    discovered_rules: List[AssociationRuleSchema]
    merchandising_recommendations: List[MerchandisingAction]