# CIS-9440-Final-Project
# Project Name: Netflix Data Warehousing
- Author(s): Dongdong Song, Tung-Ting Wang, Yan Sun
- Date created: 05/15/2021
- Class: CIS 9440

Project Objective: Follow the Kimball Lifecycle to design and develop a public, cloud-based Data Warehouse with a functioning BI Applications

Project Tools:
The tools used to build this Data Warehouse were: (change this to make applicable to your project)
1. For data integration - python
2. For data warehousing - Google BigQuery
3. For Business Intelligence - Tableau

## Kimball Lifecycle Project Stages

### Project Planning

Motivation for project:
The unexpected pandemic brings uncountable negative influence on both humans and enterprises. However, some corporations obtain some opportunities in such adverse situations, and even increase their revenue. Netflix is one of these members beyond doubt. Their annual revenue increased from $ 1,866 million to $ 2,761 million from 2019 to 2020. We would like to figure out how Netflix runs pre and during the pandemic and realize the specific conditions for each business part of Netflix in different regions during this period. 

Description of the issues or opportunities the project will address:
Our project will address and explain Netflix’s stale problems before pandemic spread such as:  Content spending not adding enough subscribers; Still reliant on licensed content-which it is losing; Spending big check to sign big name writers and producers which bring higher risk, price power evaporating and ramping up competition. 

Then, we will make a comparison between pre-Covid-19 and during Covid-19 period to figure out how Netflix deals with the problems, did the company change any strategies or will the growth rate be sustainable or not and how do they maintain their momentum post Covid-19.

Also, Netflix is always cautious about advertising on platforms, this an opportunity to expand its advertising business line. We are anticipating finding some clues after making the data warehousing and analytic. 

Project Business or Organization Value:
After realizing the clear conditions of core business in different regions during different periods. The company can easily distribute and relocate their resources and adjust some focuses or strategies in some specific regions to attract more subscribers or provide algorithms for advertising on platforms. 

Data Sources:

1.Netflix Revenue and Subscription by year and region
https://www.kaggle.com/pariaagharabi/netflix2020?select=NetflixsRevenue2018toQ2_2020.csv

2.COVID-19 Dataset: Number of confirmed, death, and recovered cases every day across globe
https://www.kaggle.com/imdevskp/corona-virus-report?select=worldometer_data.csv

### Business Requirements Definition

List of Data Warehouse KPI's:
1. Average revenue per membership by region by quarter
2. Revenue growth rate by region to Covid cases growth quarterly 
3. Netflix stock price movement to Covid cases growth weekly
4. Covid cases growth to subscribers growth rate (quarterly)
5. Netflix trend growth by month by region


### Dimensional Model

This project's Dimensional Model consists of (4) Facts and (2) Dimensions

Dimensional Model:

<img width="332" alt="9440" src="https://user-images.githubusercontent.com/84057952/118374145-8b2b9100-b588-11eb-8034-9b3f0bac1d4a.PNG">

This project's Kimball Bus Matrix:

<img width="465" alt="BUS Matrix" src="https://user-images.githubusercontent.com/84057952/118374084-3b4cca00-b588-11eb-8527-c94f6955b894.PNG">



### Business Intelligence Design and Development

List of Visualizations for each KPI:
1. line Chart :Average revenue per membership by region by quarter

<img width="805" alt="1  Average revenue per membership by region by quarter" src="https://user-images.githubusercontent.com/84057952/118374385-d3977e80-b589-11eb-800c-8167d98f1180.png">

2.stacked bar chart and line chart:Revenue growth rate by region to Covid cases growth quarterly 

<img width="728" alt="2  Revenue growth rate by region to Covid cases growth quarterly" src="https://user-images.githubusercontent.com/84057952/118374406-ee69f300-b589-11eb-89d7-e3631d4e8e8c.png">

3.Area chart:Netflix stock price movement to Covid cases growth weekly

<img width="753" alt="3  Netflix stock price movement to Covid cases growth weekly" src="https://user-images.githubusercontent.com/84057952/118374424-0477b380-b58a-11eb-9a5b-36953ed4c0e7.png">

4.combo (LINE & BAR) chart:Covid cases growth to subscribers growth rate (quarterly)

<img width="705" alt="4  Covid cases growth to subscribers growth rate (quarterly)" src="https://user-images.githubusercontent.com/84057952/118374430-0a6d9480-b58a-11eb-907c-fee6670a13ba.png">
5.Area char:Netflix trend growth by month by region

<img width="744" alt="5  Netflix trend growth by month by region" src="https://user-images.githubusercontent.com/84057952/118374436-0f324880-b58a-11eb-91a5-2198c3c63356.png">


BI Application Wireframe design:

<img width="651" alt="BI Application Wireframe design" src="https://user-images.githubusercontent.com/84057952/118374589-fbd3ad00-b58a-11eb-9349-a0916effba48.PNG">


**Picture of final Dashboard:**

Dashboard #1:

<img width="802" alt="Dashboard 1" src="https://user-images.githubusercontent.com/84057952/118374625-3dfcee80-b58b-11eb-8012-57d5048c52ea.png">

Dashboard #2:

<img width="802" alt="Dashboard 2" src="https://user-images.githubusercontent.com/84057952/118374637-4c4b0a80-b58b-11eb-92be-ed7879e3fa72.png">

**Limitation**

Due to the mismatched time scope of our datasets:  the Netflix revenue and subscription are only released quarterly (pretty small dataset) which have a few overlaps with covid 19 cases. We try to get Netflix stock price and Trend (popularity) starting from 2016 to observe the growth condition of Netflix before covid appears and make a comparison between pre covid and in the covid situations. 

For the null value in our charts. We clean the data before we put the data on bigquery. However, due to the mismatch of time of different fact tables, when connecting them in the tableau, there exists some null values anyhow. 

The covid is an emergency event, and we are still in the Covid-period. When analyzing the influence of Covid on the business conditions of Netflix, we have limited data source and relatively small time overlap between them. Hence, we focus on some abnormal changes for Netflix before and in Covid through charts, and then research their specific conditions from their financial reports or other public relations’ material in specific periods. 



### Deployment

The project was deployed on Tableau Public: 

https://public.tableau.com/profile/yan.sun2159#!/

CIS9440_Project_README.md
Displaying CIS9440_Project_README.md.
