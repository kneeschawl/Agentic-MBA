import os
import json
import asyncio
import pandas as pd
from celery import Celery
from mlxtend.frequent_patterns import fpgrowth, association_rules

# Import all three Phase B & C agents
from agents import run_consumer_psychologist, run_visual_merchandiser, run_executive_reporter

redis_url = 'redis://127.0.0.1:6379/0'
celery_app = Celery('tasks', broker=redis_url, backend=redis_url)

@celery_app.task(bind=True, name="tasks.process_market_basket")
def process_market_basket(self, file_path: str):
    """
    Executes Phase A, B, and C while streaming progressive status updates 
    to the frontend via Celery state custom metadata.
    """
    # 1. Start Math Processing
    self.update_state(state='PROGRESS', meta={'status_msg': "Parsing dataset & extracting transactions..."})
    
    df = pd.read_csv(file_path)
    basket = (df.groupby(['TransactionID', 'Item'])['Item']
              .count().unstack().reset_index().fillna(0)
              .set_index('TransactionID'))
    basket = basket.map(lambda x: 1 if x > 0 else 0)
    
    self.update_state(state='PROGRESS', meta={'status_msg': "Running FP-Growth matrix calculations..."})
    frequent_itemsets = fpgrowth(basket, min_support=0.2, use_colnames=True)
    
    if frequent_itemsets.empty:
        return {
            "status": "complete", 
            "rules": [], 
            "psychology": [], 
            "merchandising": [],
            "report": "No frequent patterns found to analyze."
        }
        
    rules = association_rules(frequent_itemsets, metric="lift", min_threshold=1.0)
    
    rules_list = []
    for _, row in rules.iterrows():
        rules_list.append({
            "antecedents": list(row['antecedents']),
            "consequents": list(row['consequents']),
            "support": round(float(row['support']), 2),
            "confidence": round(float(row['confidence']), 2),
            "lift": round(float(row['lift']), 2)
        })

    rules_list = sorted(rules_list, key=lambda x: (x['lift'], x['confidence']), reverse=True)
    top_rules_for_agents = rules_list[:3]

    # --- PHASE B & C: Agent Orchestration Loop ---
    try:
        # 2. Run Consumer Psychologist
        self.update_state(state='PROGRESS', meta={'status_msg': "🧠 Launching Consumer Psychologist Agent (analyzing behaviors)..."})
        psychology_insights = asyncio.run(run_consumer_psychologist(top_rules_for_agents))
        
        # 3. Run Visual Merchandiser
        self.update_state(state='PROGRESS', meta={'status_msg': "📐 Launching Visual Merchandiser Agent (planning layouts)..."})
        merchandising_strategies = asyncio.run(run_visual_merchandiser(psychology_insights))
        
        # 4. Run Executive Reporter
        self.update_state(state='PROGRESS', meta={'status_msg': "👔 Launching CSO Executive Reporter (synthesizing report)..."})
        combined_intelligence = {
            "rules": top_rules_for_agents,
            "psychology": psychology_insights,
            "merchandising": merchandising_strategies
        }
        executive_report = asyncio.run(run_executive_reporter(combined_intelligence))
        
    except Exception as e:
        import traceback
        print("\n" + "="*50)
        print("!!! DETAILED AGENT ERROR BREAKDOWN !!!")
        traceback.print_exc()
        print("="*50 + "\n")
        
        psychology_insights = [{"error": f"Psychologist agent failed: {str(e)}"}]
        merchandising_strategies = [{"error": f"Merchandiser agent failed: {str(e)}"}]
        executive_report = f"# System Error\nFailed to compile executive report due to exception: {str(e)}"

    # 5. Finished
    self.update_state(state='PROGRESS', meta={'status_msg': "✨ Compiling payload..."})
    return {
        "status": "complete",
        "rules": rules_list,
        "psychology": psychology_insights,
        "merchandising": merchandising_strategies,
        "report": executive_report
    }