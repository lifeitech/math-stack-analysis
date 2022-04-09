# Math Stack Exchange Data Analysis

Repository for the Business Analytics course project. I analyzed the [Mathematics Stack Exchange](https://math.stackexchange.com/) website data. I built a logistic regression model to predict user reputations based on their answer and question data. The report can be found [here](report.pdf).

### Data

The data is downloaded from [Stack Exchange Data Explorer](https://data.stackexchange.com/help). At the time of the analysis, the website has around 465K users, and 2.4 million posts.

### Code

* [data-extraction.py](code/data-extraction.py)
* [data-aggregation.py](code/data-aggregation.py)
* [eda.py](code/eda.py)
* [model.py](code/model.py)