**Consumer Credit Risk & Customer Stress Analytics**

End-to-end analytics project demonstrates how SQL + Tableau can be used to monitor loan portfolio risk, identify product concentration, and use customer complaints as an early-warning signal for emerging credit stress.

**Project Objective**

Financial institutions often monitor credit performance after delinquencies happen.
This project focuses on detecting leading indicators of stress, combining:
• portfolio exposure data
•	borrower segmentation
•	macroeconomic context
•	customer complaint trends
…to help risk teams respond earlier and more precisely.

**Dashboards Included**

1.	 **Executive Risk & Macro Sensitivity**
•	Overall exposure and high-risk percentage
•	Behavioral indicators
•	Unemployment overlay for macro context

2.	**Risk Segmentation & Concentration**
•	Heat-map of risk by Income × Grade
•	Product exposure concentration (treemap)
•	Interest-rate vs risk scatterplot

3.	 **Customer Complaints & Stress Signals**
•	MoM complaint change
•	Top complaint product + concentration
•	Complaint intensity heat-map
•	Long-term volume trend
These dashboards work together to show where risk originates, concentrates, and begins to surface through operational indicators.

 **Key Insights (Highlights)**
 
•	~19–20% of portfolio exposure is classified high-risk at origination.
•	Debt consolidation and credit cards account for most concentration risk.
•	Complaint activity spikes align closely with elevated-risk product lines.
•	Risk is more behavior-driven than macro-driven in this dataset.

Full interpretation and recommendations are included in the Executive Summary.

 **How to Reproduce**
 
1.	Create the database using scripts in /sql
2.	Load datasets (see /data/README.md)
3.	Run the BI view scripts
4.	Open the Tableau workbook in /tableau and connect to your DB
5.	Refresh dashboard extracts
   
 **Data Notice**
 
Large raw datasets are not included in this repository due to size.
All logical structures (views, schemas, and dashboard configs) are provided so the project can be recreated.

**Business Value**

This project demonstrates how data teams can:
•	detect portfolio stress earlier
•	understand where risk clusters
•	align underwriting, pricing, and servicing strategies
•	incorporate complaints into risk monitoring
It is designed to resemble real-world analytics work rather than a classroom exercise.

**Author**

Sree Theja Ayaluri

MS Information Systems | Data & Business Analytics

LinkedIn: https://www.linkedin.com/in/sree-theja-33a079191/

**License**

This project is released under the MIT License.

