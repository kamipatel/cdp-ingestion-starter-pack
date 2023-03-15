SELECT COUNT( kamcdpdemov1__Patient_Vitals__dlm.kamcdpdemov1__bp__c ) as kamcdpdemov1__bpcount__c,
kamcdpdemov1__Patient_Vitals__dlm.kamcdpdemov1__id__c as kamcdpdemov1__patientid__c,
WINDOW.START as kamcdpdemov1__start__c,
WINDOW.END as kamcdpdemov1__end__c
FROM 
kamcdpdemov1__Patient_Vitals__dlm 
GROUP BY
window( kamcdpdemov1__Patient_Vitals__dlm.kamcdpdemov1__created__c ,'5 MINUTE'),
kamcdpdemov1__patientid__c

