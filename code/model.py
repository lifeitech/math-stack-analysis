import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import rcParams
rcParams['font.family'] = 'sans-serif'
rcParams['font.sans-serif'] = ['Optima']
from sklearn.metrics import roc_curve, auc
from sklearn.model_selection import StratifiedKFold
from scipy import interp
import statsmodels.api as sm

# read data
table = pd.read_csv('/Users/francis/Desktop/data/table.csv')

# Standardize features and retain dataframe
features = table[['Answer_Counts', 'Question_Counts', 'Average_OP_Reputation',
           'Average_Post_Length', 'Average_Math_Ratio', 'response_time_seconds']]
normalized_features=(features-features.min())/(features.max()-features.min())

# Logistic regression with standardized features from dataframe
reg_table = pd.concat([normalized_features, table['top_user']],axis=1)
model = sm.Logit.from_formula('top_user ~ Answer_Counts + Question_Counts + Average_OP_Reputation + Average_Post_Length + Average_Math_Ratio + response_time_seconds', data=reg_table)    
result = model.fit()
print(result.summary())

# ROC curve and AUC score
fpr, tpr, thresholds = roc_curve(table.top_user, result.predict())
roc_auc = auc(fpr,tpr)
print(roc_auc)

plt.plot(fpr, tpr, color='darkorange', lw=2, label='ROC curve (area = %0.4f)' % roc_auc)
plt.plot([0, 1], [0, 1], color='navy', lw=2, linestyle='--')
plt.xlim([0.0, 1.0])
plt.ylim([0.0, 1.05])
plt.xlabel('False Positive Rate')
plt.ylabel('True Positive Rate')
plt.legend(loc="lower right")
plt.title('Receiver Operating Characteristic (ROC)')
plt.tight_layout()
plt.show()

# ROC and AUC under 10-fold cross-validation
cv = StratifiedKFold(n_splits=10)
tprs = []
aucs = []
mean_fpr = np.linspace(0, 1, 100)
X = normalized_features.values
y = table.top_user

i = 0
for train, test in cv.split(X, y):
    probas_ = sm.Logit(y[train], X[train]).fit(disp=0).predict(X[test])
    # Compute ROC curve and area the curve
    fpr, tpr, thresholds = roc_curve(y[test], probas_)  # probas_[:, 1]
    tprs.append(interp(mean_fpr, fpr, tpr))
    tprs[-1][0] = 0.0
    roc_auc = auc(fpr, tpr)
    aucs.append(roc_auc)
    plt.plot(fpr, tpr, lw=1, alpha=0.3, label='ROC fold %d (AUC = %0.2f)' % (i, roc_auc))
    i = i + 1

plt.plot([0, 1], [0, 1], linestyle='--', lw=2, color='r',
         label='Chance', alpha=.8)

mean_tpr = np.mean(tprs, axis=0)
mean_tpr[-1] = 1.0
mean_auc = auc(mean_fpr, mean_tpr)
std_auc = np.std(aucs)
plt.plot(mean_fpr, mean_tpr, color='b',
         label=r'Mean ROC (AUC = %0.2f $\pm$ %0.2f)' % (mean_auc, std_auc),
         lw=2, alpha=.8)

std_tpr = np.std(tprs, axis=0)
tprs_upper = np.minimum(mean_tpr + 1*std_tpr, 1)
tprs_lower = np.maximum(mean_tpr - 1*std_tpr, 0)
plt.fill_between(mean_fpr, tprs_lower, tprs_upper, color='grey', alpha=.2,
                 label=r'$\pm$ 1 std. dev.')

plt.xlim([-0.05, 1.05])
plt.ylim([-0.05, 1.05])
plt.xlabel('False Positive Rate')
plt.ylabel('True Positive Rate')
plt.title('Receiver Operating Characteristic (ROC)')
plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))

plt.show()