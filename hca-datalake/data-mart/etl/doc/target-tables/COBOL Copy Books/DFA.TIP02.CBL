       IDENTIFICATION DIVISION.
       PROGRAM-ID. DIRECT-FINANCIAL-ACTIVITY.
	   AUTHOR. DST. MODIFIED BY SHREENI.
	   DATE WRITTEN. 07/01/2009.
	   DATE COMPILED. 07/01/2009.
       ENVIRONMENT DIVISION.
       CONFIGURATION SECTION.
       FILE-CONTROL.
	    SELECT DIRECT-FINANCIAL-ACTIVITY ASSIGN TO "FNAME".
       DATA DIVISION.
       FILE SECTION.
       FD  DIRECT-FINANCIAL-ACTIVITY.
       01  DIRECT-FINANCIAL-ACTIVITY.                                                             
           05  RECORD-TYPE                       PIC X(3).                      
               88  DIRECT-FINANCIAL-ACTIVITY         VALUE 'DFA'.               
           05  SEQUENCE-NUMBER                   PIC 9(3).                      
           05  DETAIL-RECORD-1.                                                 
               10  RECORD-CODE                   PIC 9(3).                      
               10  FINS-ID                       PIC 9(7).                      
               10  DEALER-BRANCH-ID              PIC X(9).                      
               10  CUSIP-NUMBER                  PIC X(9).                      
               10  FUND-CODE                     PIC 9(7).                      
               10  CUSTOMER-ACCOUNT-NUMBER       PIC X(20).                     
               10  ACCOUNT-NUMBER-CODE           PIC X(1).                      
                   88  ACCOUNT-NUMBER-FUND           VALUE 'F'.                 
                   88  ACCOUNT-NUMBER-EXTERNAL       VALUE 'E'.                 
               10  BATCH-NUMBER                  PIC 9(7).                      
               10  BATCH-DATE.                                                  
                   15  BATCH-YYYY-DATE.                                         
                       20  BATCH-CC-DATE         PIC 9(2).                      
                       20  BATCH-YY-DATE         PIC 9(2).                      
                   15  BATCH-MM-DATE             PIC 9(2).                      
                   15  BATCH-DD-DATE             PIC 9(2).                      
               10  TRANSACTION-CODE              PIC 9(3).                      
               10  TRANSACTION-SUFFIX            PIC 9(3).                      
               10  SHARE-BALANCE-EFFECT-CODE     PIC X(1).                      
                   88  SHARE-BALANCE-ADD             VALUE 'A'.                 
                   88  SHARE-BALANCE-SUBTRACT        VALUE 'S'.                 
                   88  SHARE-BALANCE-NO-EFFECT       VALUE 'N'.                 
               10  PER-SHARE-PRICE-AMOUNT        PIC 9(5)V9(4).                 
               10  TRANSACTION-GROSS-AMOUNT      PIC 9(13)V9(2).                
               10  TRANSACTION-SHARES-COUNT      PIC 9(11)V9(4).                
               10  TRADE-DATE.                                                  
                   15  TRADE-YYYY-DATE.                                         
                       20  TRADE-CC-DATE         PIC 9(2).                      
                       20  TRADE-YY-DATE         PIC 9(2).                      
                   15  TRADE-MM-DATE             PIC 9(2).                      
                   15  TRADE-DD-DATE             PIC 9(2).                      
               10  CONFIRM-DATE.                                                
                   15  CONFIRM-YYYY-DATE.                                       
                       20  CONFIRM-CC-DATE       PIC 9(2).                      
                       20  CONFIRM-YY-DATE       PIC 9(2).                      
                   15  CONFIRM-MM-DATE           PIC 9(2).                      
                   15  CONFIRM-DD-DATE           PIC 9(2).                      
               10  DISCOUNT-CATEGORY             PIC 9(2).                      
               10  ORDER-NUMBER                  PIC 9(9).                      
               10  ACCOUNT-TYPE-CODE             PIC X(1).                      
                   88  ACCOUNT-TYPE-NEW              VALUE '0'.                 
                   88  ACCOUNT-TYPE-EXISTING         VALUE '1'.                 
               10  DEALER-CONTROL-LEVEL-CODE     PIC 9(1).                      
                   88  DEALER-CONTROL-NON-NETWORKING VALUE 0.                   
                   88  DEALER-CONTROL-BRKR-MARGIN-IRA                           
                                                     VALUE 1.                   
                   88  DEALER-CONTROL-CUST-NAME-BRKR VALUE 2.                   
                   88  DEALER-CONTROL-BROKERAGE-BRKR VALUE 3.                   
                   88  DEALER-CONTROL-BROKERAGE-FUND VALUE 4.                   
               10  PAYMENT-METHOD-CDE            PIC X(1).                      
                   88  PAYMENT-METHOD-ACH            VALUE 'A'.                 
                   88  PAYMENT-METHOD-CHECK          VALUE 'C'.                 
                   88  PAYMENT-METHOD-DRAFT          VALUE 'D'.                 
                   88  PAYMENT-METHOD-ACH-SPECIAL    VALUE 'S'.                 
                   88  PAYMENT-METHOD-WIRE           VALUE 'W'.                 
                   88  PAYMENT-METHOD-CLEARING-AGENT VALUE '1'.                 
                   88  PAYMENT-METHOD-NA             VALUE 'Z'.                 
               10  PRE-POST-NOON                 PIC 9(1).                      
                   88  POST-NOON                     VALUE 0.                   
                   88  PRE-NOON                      VALUE 1.                   
               10  NSCC-STTR-WV-ID               PIC X(1).                      
               10  DTL-REC-TYP-8-TOT-REC-CNT     PIC 9(5).                      
           05  DETAIL-RECORD-2 REDEFINES DETAIL-RECORD-1.                       
               10  CUMULATIVE-DISCOUNT-NUMBER    PIC 9(9).                      
               10  LOI-NUMBER                    PIC 9(9).                      
               10  SOCIAL-CODE                   PIC 9(3).                      
               10  RESIDENT-STATE-CODE           PIC 9(3).                      
               10  REPRESENTATIVE-NUMBER         PIC X(9).                      
               10  REPRESENTATIVE-NAME           PIC X(30).                     
               10  SALES-CHARGE-PERCENT          PIC 9(5)V9(10).                
               10  DEALER-COMMISSION-CODE        PIC X(1).                      
                   88  DEALER-COMMISSION-GROSS-ALPHA VALUE 'G'.                 
                   88  DEALER-COMMISSION-NET-ALPHA   VALUE 'N'.                 
                   88  DEALER-COMMISSION-GROSS-NBR   VALUE '1'.                 
                   88  DEALER-COMMISSION-NET-NBR     VALUE '2'.                 
               10  DEALER-COMMISSION-AMOUNT      PIC 9(13)V9(2).                
               10  UNDERWRITER-COMMISSION-AMOUNT PIC 9(13)V9(2).                
               10  AS-OF-CODE                    PIC 9(3).                      
               10  ISSUE-CODE                    PIC 9(1).                      
               10  CHECK-NUMBER                  PIC 9(13).                     
               10  SOCIAL-SECURITY-NBR           PIC 9(9).                      
               10  SOCIAL-SECURITY-NBR-STATUS-CDE                               
                                                 PIC 9(1).                      
                   88  SS-MISSING                    VALUE 0.                   
                   88  SSN-NON-CERTIFIED             VALUE 1.                   
                   88  SSN-CERTIFIED                 VALUE 2.                   
                   88  TIN-NON-CERTIFIED             VALUE 3.                   
                   88  TIN-CERTIFIED                 VALUE 4.                   
                   88  SS-APPLIED                    VALUE 5.                   
                   88  SSN-IRS-OFFENDER              VALUE 6.                   
                   88  TIN-IRS-OFFENDER              VALUE 7.                   
                   88  SSN-ACCOUNT-TYPE-EXEMPT       VALUE 8.                   
                   88  TIN-ACCOUNT-TYPE-EXEMPT       VALUE 9.                   
                   88  FIRST-SURNAME                 VALUE 1 2 6 8.             
                   88  ID-NUMBER                     VALUE 3 4 7 9.             
               10  NAV-CODE                       PIC X(1).                      
                   88  NAV-NO                        VALUE '0'.                 
                   88  NAV-YES                       VALUE '1' THRU             
           '9'.                                      
                   88  NAV-YES                       VALUE 'A' THRU             
           'Z'.
               10  SHR-ACCT-PRICE-SCHED-CDE      PIC 9(1).                      
                   88  SHR-ACCT-PRICE-SCHED-REDUCED  VALUE 1.                   
                   88  SHR-ACCT-PRICE-SCHED-REGULAR  VALUE 2.                   
               10  SHR-ACCT-MGMT-CO-EMPLOYEE-CDE PIC X(1).                      
                   88  SHR-ACCT-MGMT-CO-EMPLOYEE-YES VALUE 'Y'.                 
                   88  SHR-ACCT-MGMT-CO-EMPLOYEE-NO  VALUE 'N'.                 
               10  EXTERNAL-PLAN-ID              PIC X(9).                      
               10  UNDERWRITER-COMM-EFFECT-CDE   PIC X(1).                      
                   88  UNDERWRITER-COMM-EFFECT-ADD   VALUE '+'.                 
                   88  UNDERWRITER-COMM-EFFECT-SUB   VALUE '-'.                 
               10  FIN-TR-CDSC-WV-RSN            PIC X(4).                      
               10  COMMISSIONABLE-SHARES-CD      PIC X(1).                      
                   88  COMMISSIONABLE-SHARES-YES     VALUE 'Y'.                 
                   88  COMMISSIONABLE-SHARES-NO      VALUE 'N'.                 
           05  DETAIL-RECORD-3 REDEFINES DETAIL-RECORD-1.                       
               10  CONTRIBUTION-YEAR             PIC 9(4).                      
               10  FUND-FROM-TO                  PIC 9(7).                      
               10  ACCOUNT-FROM-TO               PIC 9(11).                     
               10  VOL-TRAN-DESC                 PIC X(26).                     
               10  FINS-ACCOUNT-ID               PIC X(20).                     
               10  FINS-ACCOUNT-CDE              PIC X(1).                      
                   88  FINS-ACCOUNT-FUND             VALUE 'F'.                 
                   88  FINS-ACCOUNT-EXTERNAL         VALUE 'E'.                 
               10  SUPER-SHEET-DTE.                                             
                   15  SUPER-SHEET-YYYY-DTE.                                    
                       20  SUPER-SHEET-CC-DTE    PIC 9(2).                      
                       20  SUPER-SHEET-YY-DTE    PIC 9(2).                      
                   15  SUPER-SHEET-MM-DTE        PIC 9(2).                      
                   15  SUPER-SHEET-DD-DTE        PIC 9(2).                      
               10  BANK-MICR-ID.                                                
                   15  BANK-FED-RESERVE-ROUTING-ID                              
                                                 PIC 9(4).                      
                   15  BANK-ABA-ID               PIC 9(4).                      
                   15  BANK-CHECK-DIGIT-ID       PIC 9(1).                      
               10  BANK-ACCT-NUMBER-UNEDITED     PIC 9(17).                     
               10  LIQUIDATION-CDE               PIC X(1).                      
                   88  LIQUIDATION-FULL              VALUE 'F'.                 
                   88  LIQUIDATION-PARTIAL           VALUE 'P'.                 
                   88  LIQUIDATION-NA                VALUE 'Z'.                 
               10  TRADE-ENTRY-METHOD-CDE        PIC X(1).                      
                   88  TRADE-ENTRY-METHOD-ONLINE     VALUE 'O'.                 
                   88  TRADE-ENTRY-METHOD-XMIT       VALUE 'T'.                 
                   88  TRADE-ENTRY-METHOD-SYSTEM     VALUE 'S'.                 
                   88  TRADE-ENTRY-METHOD-SO-SYSTEM  VALUE 'Y'.                 
                   88  TRADE-ENTRY-METHOD-TELTRAN    VALUE 'X'.                 
                   88  TRADE-ENTRY-METHOD-IWS        VALUE 'I'.                 
                   88  TRADE-ENTRY-METHOD-ARS        VALUE 'A'.                 
                   88  TRADE-ENTRY-METHOD-CONVERSION VALUE 'C'.                 
                   88  TRADE-ENTRY-MTH-SO-SYSTEM     VALUE 'Y'.                 
                   88  TRADE-ENTRY-MTH-FAN           VALUE 'F'.                 
                   88  TRADE-ENTRY-MTH-CLIENT-FE     VALUE 'E'.                 
               10  TRADE-ORIGIN-ID               PIC 9(7).                      
                   88  TRADE-ORIGIN-NA               VALUE 0000000.             
                   88  TRADE-ORIGIN-NSCC             VALUE 0000100.             
                   88  TRADE-ORIGIN-DDPS             VALUE 0000200.             
                   88  TRADE-ORIGIN-BULKS            VALUE 0000300.             
                   88  TRADE-ORIGIN-HOT-WIRES        VALUE 0000400.             
                   88  TRADE-ORIGIN-SWITCH           VALUE 0000500.             
                   88  TRADE-ORIGIN-AFS-SHARE        VALUE 0000800.             
               10  MUTUAL-FUND-TRANSACTION-ID.                                  
                   15  MUTUAL-FUND-TRANSACTION-ID-1                             
                                                 PIC 9(11).                     
                   15  MUTUAL-FUND-TRANSACTION-ID-2                             
                                                 PIC 9(11).                     
               10  TRUSTEE-NSCC-FIRM-NBR         PIC X(4).                      
               10  NSCC-TPA-FIRM-NBR             PIC X(4).                      
               10  SHR-ACCT-CLIENT-DEFINED-1-CDE PIC X(1).                      
               10  SHR-ACCT-CLIENT-DEFINED-2-CDE PIC X(1).                      
               10  SHR-ACCT-CLIENT-DEFINED-3-CDE PIC X(1).                      
               10  SHR-ACCT-CLIENT-DEFINED-4-CDE PIC X(1).                      
               10  SHR-ACCT-CLIENT-DEFINED-5-CDE PIC X(1).                      
               10  SHR-ACCT-CLIENT-DEFINED-6-CDE PIC X(1).                      
               10  SHR-ACCT-CLIENT-DEFINED-7-CDE PIC X(1).                      
               10  SHR-ACCT-CLIENT-DEFINED-8-CDE PIC X(1).                      
               10  FILLER                        PIC X(4).                      
           05  DETAIL-RECORD-4 REDEFINES DETAIL-RECORD-1.                       
               10  FI-EXTERNAL-TR-ID             PIC 9(11).                     
               10  FILLER                        PIC X(2).                      
               10  MERCHANT-DESCRIPTION-TXT      PIC X(40).                     
               10  CUSTODIAN-ID                  PIC 9(7).                      
               10  THIRD-PRTY-ADMN-ID            PIC 9(7).                      
               10  FIN-TR-ADVANCED-COMMISSION-AT PIC 9(13)V9(2).                
               10  NSCC-BRANCH-ID-NBR            PIC X(9).                      
               10  NSCC-NAV-REASON-CODE          PIC X(1).                      
                   88  NSCC-NAV-OTHER                VALUE '0'.                 
                   88  NSCC-NAV-NET-ASSET-VALUE-XFER VALUE '1'.                 
                   88  NSCC-NAV-REPURCHASE           VALUE '2'.                 
                   88  NSCC-NAV-EMPLOYEE             VALUE '3'.                 
                   88  NSCC-NAV-ERROR-CORR           VALUE '4'.                 
                   88  NSCC-NAV-WRAP-FEE-ACCOUNT     VALUE '5'.                 
               10  SHR-ACCT-CLIENT-DEFINED-1-TXT PIC X(10).                     
               10  ALPHA-CODE                    PIC X(10).                     
               10  FIRM-ACCOUNT-NBR              PIC X(20).                     
               10  SEC-ISS-ID                    PIC X(9).                      
               10  OPERATOR-ID                   PIC X(8).                      
               10  NSCC-IRA-CODE                 PIC X(1).                      
               10  FILLER                        PIC X(4).                      
           05  DETAIL-RECORD-5 REDEFINES DETAIL-RECORD-1.                       
               10  ACL-BYPASS-CD                 PIC X(1).                      
                   88  ACL-BYPASS-YES                VALUE 'Y'.                 
                   88  ACL-BYPASS-NO                 VALUE 'N'.                 
               10  INVESTMENT-LVL-AT             PIC 9(13)V9(2).                
               10  IVT-LVL-ORR-AT                PIC 9(13)V9(2).                
               10  NET-ASSET-VALUE-AT            PIC 9(13)V9(2).                
               10  PLAN-LEN-MTH-CT               PIC 9(3).                      
               10  PLAN-PROSPECTUS-ID            PIC 9(5).                      
               10  SLS-CRE-CRG-AT                PIC 9(13)V9(2).                
               10  FI-COMMISSION-RT              PIC 9(5)V9(10).                
               10  CTA-TR-TYP-ID                 PIC 9(3).                      
               10  END-IVT-NB-TR-CT              PIC 9(3)V9(6).                 
               10  ISMT-FI-COM-DCM-AT            PIC 9(13)V9(2).                
               10  ISMT-IVT-DCM-CT               PIC 9(3)V9(6).                 
               10  ISMT-SLS-CRG-DCM              PIC 9(13)V9(2).                
               10  FILLER                        PIC X(19).                     
           05  DETAIL-RECORD-6-AREA REDEFINES DETAIL-RECORD-1.                  
               10  NSCC-CONTROL-NUMBER           PIC X(15).                     
               10  NSCC-CORRECTION-CODE          PIC X(1).                      
                   88  NSCC-CORRECTION-NO            VALUE SPACE.               
                   88  NSCC-CORRECTION-YES           VALUE 'C' THRU             
           'N'.                                                                 
                   88  NSCC-CORRECTION-POST-STL      VALUE 'P'.                 
               10  AFL-FI-ID                     PIC 9(7).                      
               10  AFL-FI-BR-ID                  PIC X(9).                      
               10  AFL-FI-BR-REP-ID              PIC X(9).                      
               10  AFL-FI-PCS-ROL-CD             PIC X(1).                      
                   88  AFL-FI-PCS-ROL-CLEARING       VALUE 'C'.                 
                   88  AFL-FI-PCS-ROL-EXECUTING      VALUE 'E'.                 
                   88  AFL-FI-PCS-ROL-NA             VALUE 'Z'.                 
               10  FILLER                        PIC X(112).                    
           05  DETAIL-RECORD-7-AREA REDEFINES DETAIL-RECORD-1.                  
               10  FUND-CODE                     PIC 9(7).                      
               10  ACCOUNT-NUMBER                PIC 9(11).                     
               10  FIRM-ACCOUNT-NBR              PIC X(20).                     
               10  SEC-ISS-ID                    PIC X(9).                      
               10  TICKER-SYMBOL-ID              PIC X(5).                      
               10  TA-MUTUAL-FUND-ID             PIC X(20).                     
               10  EXTL-OMB-MFA-NBR              PIC X(20).                     
               10  OMB-FIRM-ACCT-NBR             PIC X(20).                     
               10  JUMBO-INVEST-AMT              PIC 9(15)V9(2).                
               10  NSCC-TRANS-TYPE               PIC X(1).                      
               10  NSCC-TRANS-CODE               PIC X(2).                      
                   88  INITIAL-PURCHASE              VALUE '01'.                
                   88  SUBSEQUENT-PURCHASE           VALUE '02'.                
                   88  PARTIAL-LIQUIDATION           VALUE '03'.                
                   88  FULL-LIQUIDATION              VALUE '04'.                
                   88  FULL-EXCHANGE                 VALUE '05'.                
                   88  PARTIAL-EXCHANGE              VALUE '06'.                
                   88  PST-STL-CORRECTION-PURCHASE   VALUE '07'.                
                   88  PST-STL-CORRECTION-LIQUIDATION                           
                                                     VALUE '08'.                
                   88  PST-STL-EXIT                  VALUE '09'.                
                   88  CASH-ADJ-DEBIT-FIRM           VALUE 'DB'.                
                   88  CASH-ADJ-CREDIT-FIRM          VALUE 'CR'.                
               10  NSCC-ROA-LOI-VALUE            PIC 9(13)V9(2).                
               10  FILLER                        PIC X(7).                      
           05  DETAIL-RECORD-8-AREA REDEFINES DETAIL-RECORD-1.                  
               10  DTL-REC-TYP-8-SEQUENCE-CT     PIC 9(9).                      
               10  MF-TRAN-2-ID                  PIC 9(11).                     
               10  MF-TRAN-1-ID                  PIC 9(11).                     
               10  CMPN-TYPE-ID                  PIC 9(3).                      
               10  FUND-SPONSOR-ID               PIC 9(5).                      
               10  FS-COM-FEE-TYPE-ID            PIC X(4).                      
               10  TR-COM-FEE-FI-RT              PIC 9(5)V9(10).                
               10  TR-COM-FEE-FI-AT              PIC 9(13)V9(2).                
               10  TR-COM-FEE-FI-EFF-CD          PIC X(1).                      
                   88  TR-COM-FEE-FI-EFF-POS         VALUE '+'.                 
                   88  TR-COM-FEE-FI-EFF-NEG         VALUE '-'.                 
               10  TR-COM-FEE-UWR-RT             PIC 9(5)V9(10).                
               10  TR-COM-FEE-UWR-AT             PIC 9(13)V9(2).                
               10  TR-COM-FEE-UWR-EFF-CD         PIC X(1).                      
                   88  TR-COM-FEE-UWR-EFF-POS        VALUE '+'.                 
                   88  TR-COM-FEE-UWR-EFF-NEG        VALUE '-'.                 
               10  FILLER                        PIC X(49).                     
           05  DETAIL-RECORD-9-AREA REDEFINES DETAIL-RECORD-1.                  
               10  NSCC-FIRM-NBR                 PIC X(4).                      
               10  NSCC-AGENT-FOR-FIRM-NBR       PIC X(4).                      
               10  OMB-NSCC-CONTROL-NBR          PIC X(15).                     
               10  FILLER                        PIC X(131).                    
           05  DETAIL-RECORD-10-AREA REDEFINES DETAIL-RECORD-1.                 
               10  FINS-CLASSIFICATION-ABR-NM    PIC X(4).                      
               10  CSTD-CLASSIFICATION-ABR-NM    PIC X(4).                      
               10  TPA-CLASSIFICATION-ABR-NM     PIC X(4).                      
               10  FILLER                        PIC X(142).   
       WORKING-STORAGE SECTION.
       PROCEDURE DIVISION.
        STOP RUN.			   
