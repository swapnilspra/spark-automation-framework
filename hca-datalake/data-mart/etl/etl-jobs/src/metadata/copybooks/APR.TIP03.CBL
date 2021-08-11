       IDENTIFICATION DIVISION.
       PROGRAM-ID. ACCOUNT-POSITION.
	   AUTHOR. DST. MODIFIED BY SHREENI.
	   DATE WRITTEN. 07/01/2009.
	   DATE COMPILED. 07/01/2009.
       ENVIRONMENT DIVISION.
       CONFIGURATION SECTION.
       FILE-CONTROL.
	    SELECT ACCOUNT-POSITION ASSIGN TO "FNAME".
       DATA DIVISION.
       FILE SECTION.
       FD  ACCOUNT-POSITION.
           01  ACCOUNT-POSITION.                                                         
           05  RECORD-TYPE                       PIC X(3).                      
               88  ACCOUNT-ACTIVITY                  VALUE 'APR'.               
               88  ACCOUNT-POSITION                  VALUE 'AAR'.               
           05  SEQUENCE-NUMBER                   PIC 9(3).                      
           05  DETAIL-RECORD-1.                                                 
               10  FINS-ID                       PIC 9(7).                      
               10  DEALER-BRANCH-ID              PIC X(9).                      
               10  CUSIP-NUMBER                  PIC X(9).                      
               10  FUND-CODE                     PIC 9(7).                      
               10  CUSTOMER-ACCOUNT-NUMBER       PIC X(20).                     
               10  ACCOUNT-NUMBER-CODE           PIC X(1).                      
                   88  ACCOUNT-NUMBER-FUND           VALUE 'F'.                 
                   88  ACCOUNT-NUMBER-EXTERNAL       VALUE 'E'.                 
               10  TOTAL-SHARES-COUNT            PIC 9(11)V9(4).                
               10  UNISSUED-SHARES-COUNT         PIC 9(11)V9(4).                
               10  ISSUED-SHARES-COUNT           PIC 9(11)V9(4).                
               10  NET-ASSET-VALUE               PIC 9(5)V9(4).                 
               10  RESERVED-SHARE-CNT            PIC 9(11)V9(4).                
               10  TRUSTEE-NSCC-FIRM-NBR         PIC X(4).                      
               10  NSCC-TPA-FIRM-NBR             PIC X(4).                      
               10  CUSTODIAN-ID                  PIC 9(7).                      
               10  THIRD-PRTY-ADMN-ID            PIC 9(7).                      
               10  SHR-ACCT-PPA-CONTROLLED-CDE   PIC X(1).                      
                   88  SHR-ACCT-PPA-CONTROLLED-YES   VALUE 'Y'.                 
                   88  SHR-ACCT-PPA-CONTROLLED-NO    VALUE 'N'.                 
               10  FILLER                        PIC X(9).                      
           05  DETAIL-RECORD-2 REDEFINES DETAIL-RECORD-1.                       
               10  ACCRUED-DIVIDEND-AMOUNT       PIC 9(13)V9(2).                
               10  ESCROW-SHARES-COUNT           PIC 9(11)V9(4).                
               10  COLLECTED-BALANCE-AMOUNT      PIC 9(13)V9(2).                
               10  ALPHA-CODE                    PIC X(10).                     
               10  REPRESENTATIVE-NUMBER         PIC X(9).                      
               10  REPRESENTATIVE-NAME           PIC X(30).                     
               10  CUMULATIVE-DISCOUNT-NUMBER    PIC 9(9).                      
               10  SOCIAL-CODE                   PIC 9(3).                      
               10  DEALER-CONTROL-LEVEL-CODE     PIC X(1).                      
                   88  NON-NETWORKED-BROKER-ACCOUNT  VALUE '0'.                 
                   88  BROKER-CONTROL-IRA-MARGIN-ACCT                           
                                                     VALUE '1'.                 
                   88  CUSTOMER-NAME-BROKERAGE-ACCT  VALUE '2'.                 
                   88  BROKER-CONTROL-BROKERAGE-ACCT VALUE '3'.                 
                   88  FUND-CONTROLLED-BROKERAGE-ACCT                           
                                                     VALUE '4'.                 
               10  LOI-NUMBER                    PIC 9(9).                      
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
               10  SHR-ACCT-PRICE-SCHED-CDE      PIC X(1).                      
                   88  SHR-ACCT-PRICE-SCHED-REDUCED  VALUE '1'.                 
                   88  SHR-ACCT-PRICE-SCHED-REGULAR  VALUE '2'.                 
               10  SHR-ACCT-MGMT-CO-EMPLOYEE-CDE PIC X(1).                      
                   88  SHR-ACCT-MGMT-CO-EMPLOYEE-YES VALUE 'Y'.                 
                   88  SHR-ACCT-MGMT-CO-EMPLOYEE-NO  VALUE 'N'.                 
               10  EXTERNAL-PLAN-ID              PIC X(9).                      
               10  COLLECTED-SHARES-CNT          PIC 9(11)V9(4).                
               10  FILLER                        PIC X(1).                      
           05  DETAIL-RECORD-3 REDEFINES DETAIL-RECORD-1.                       
               10  CUR-INCOME-DIVIDENDS          PIC 9(13)V9(2).                
               10  CUR-LONG-TERM-CAP-GAINS       PIC 9(13)V9(2).                
               10  CUR-SHORT-TERM-CAP-GAINS      PIC 9(13)V9(2).                
               10  CUR-FOREIGN-WITHHOLDING       PIC 9(13)V9(2).                
               10  CUR-DIVIDEND-WITHHOLDING      PIC 9(13)V9(2).                
               10  CUR-FOREIGN-TAX-PAID-AMT      PIC 9(13)V9(2).                
               10  COLLECTED-SHARES-CHANGE-CDE   PIC X(1).                      
               10  COLLECTED-SHARES-NET-CHG-CNT  PIC 9(11)V9(4).                
               10  COLLECTED-SHARES-NET-CHG-CDE  PIC X(1).                      
               10  STOP-TRANSFER-CDE             PIC X(1).                      
               10  STOP-TRANSFER-CHANGE-CDE      PIC X(1).                      
               10  TOTAL-SHARES-NET-CHG-CNT      PIC 9(11)V9(4).                
               10  TOTAL-SHARES-NET-CHG-CDE      PIC X(1).                      
               10  FINS-ACCOUNT-ID               PIC X(20).                     
               10  FINS-ACCOUNT-CDE              PIC X(1).                      
                   88  FINS-ACCOUNT-FUND             VALUE 'F'.                 
                   88  FINS-ACCOUNT-EXTERNAL         VALUE 'E'.                 
               10  SHR-ACCT-CLIENT-DEFINED-1-CDE PIC X(1).                      
               10  SHR-ACCT-CLIENT-DEFINED-2-CDE PIC X(1).                      
               10  SHR-ACCT-CLIENT-DEFINED-3-CDE PIC X(1).                      
               10  SHR-ACCT-CLIENT-DEFINED-4-CDE PIC X(1).                      
               10  SHR-ACCT-CLIENT-DEFINED-5-CDE PIC X(1).                      
               10  SHR-ACCT-CLIENT-DEFINED-6-CDE PIC X(1).                      
               10  SHR-ACCT-CLIENT-DEFINED-7-CDE PIC X(1).                      
               10  SHR-ACCT-CLIENT-DEFINED-8-CDE PIC X(1).                      
           05  DETAIL-RECORD-4 REDEFINES DETAIL-RECORD-1.                       
               10  PREV-INCOME-DIVIDENDS         PIC 9(13)V9(2).                
               10  PREV-LONG-TERM-CAP-GAINS      PIC 9(13)V9(2).                
               10  PREV-SHORT-TERM-CAP-GAINS     PIC 9(13)V9(2).                
               10  PREV-FOREIGN-WITHHOLDING      PIC 9(13)V9(2).                
               10  PREV-DIVIDEND-WITHHOLDING     PIC 9(13)V9(2).                
               10  PREV-FOREIGN-TAX-PAID-AMT     PIC 9(13)V9(2).                
               10  1099B-GROSS-REDEMPTION-AMT    PIC 9(13)V9(2).                
               10  1099B-GROSS-WITHHOLD-AMT      PIC 9(13)V9(2).                
               10  AFL-FI-ID                     PIC 9(7).                      
               10  AFL-FI-BR-ID                  PIC X(9).                      
               10  AFL-FI-BR-REP-ID              PIC X(9).                      
               10  AFL-FI-PCS-ROL-CD             PIC X(1).                      
                   88  AFL-FI-PCS-ROL-CLEARING       VALUE 'C'.                 
                   88  AFL-FI-PCS-ROL-EXECUTING      VALUE 'E'.                 
                   88  AFL-FI-PCS-ROL-NA             VALUE 'Z'.                 
               10  FILLER                        PIC X(8).                      
           05  DETAIL-RECORD-5-AREA REDEFINES DETAIL-RECORD-1.                  
               10  DSG-WTHH-AMT                  PIC 9(13)V9(2).                
               10  DSG-WTHH-RTE                  PIC 9(5)V9(10).                
               10  DIVIDEND-WTH-AT               PIC 9(13)V9(2).                
               10  DIVIDEND-WTH-RT               PIC 9(5)V9(10).                
               10  STCG-WTH-AT                   PIC 9(13)V9(2).                
               10  STCG-WTH-RT                   PIC 9(5)V9(10).                
               10  LTCG-WTH-AT                   PIC 9(13)V9(2).                
               10  LTCG-WTH-RT                   PIC 9(5)V9(10).                
               10  FILLER                        PIC X(34).                     
           05  DETAIL-RECORD-6-AREA REDEFINES DETAIL-RECORD-1.                  
               10  FUND-GRP-NUMBER               PIC 9(7).                      
               10  ACCOUNT-GROUP-ID              PIC 9(11).                     
               10  PLAN-ESTABLISH-DT             PIC X(10).                     
               10  MAX-ACL-IVT-ALW-CT            PIC 9(3)V9(6).                 
               10  NEXT-INVESTMENT-DT            PIC X(10).                     
               10  PLAN-LEN-MTH-CT               PIC 9(3).                      
               10  LAST-WITHDRAWN-DT             PIC X(10).                     
               10  CASH-WITHDRAWN-AT             PIC 9(13)V9(2).                
               10  REFUNDABLE-FEE-AT             PIC 9(7)V9(2).                 
               10  GROSS-AT                      PIC 9(13)V9(2).                
               10  NET-INVESTED-AT               PIC 9(13)V9(2).                
               10  IVT-LVL-ORR-AT                PIC 9(13)V9(2).                
               10  INVESTMENT-LVL-AT             PIC 9(13)V9(2).                
               10  FILLER                        PIC X(10).                     
           05  DETAIL-RECORD-7-AREA REDEFINES DETAIL-RECORD-1.                  
               10  PLAN-PROSPECTUS-ID            PIC 9(5).                      
               10  ACL-BYP-IVT-CT                PIC 9(3)V9(6).                 
               10  BEG-INVESTMENT-CT             PIC 9(3)V9(6).                 
               10  COMPLETION-STA-CD             PIC X(1).                      
                   88  COMPLETION-STA-INCOMPLETE     VALUE 'I'.                 
                   88  COMPLETION-STA-COMPLETE       VALUE 'C'.                 
                   88  COMPLETION-STA-EXT-PMNT-PLAN  VALUE 'E'.                 
               10  IVT-DUE-DAY-DT                PIC 9(2).                      
               10  FEE-REFUND-AT                 PIC 9(7)V9(2).                 
               10  INVESTMENT-DUE-CT             PIC 9(3).                      
               10  DLQ-IVT-CT                    PIC 9(3)V9(6).                 
               10  ORN-RSD-STATE-CD              PIC 9(7).                      
               10  FACE-AT                       PIC 9(13)V9(2).                
               10  REINSTATEMENT-AT              PIC 9(13)V9(2).                
               10  FI-COM-REFUND-AT              PIC 9(13)V9(2).                
               10  SLS-CRG-RFN-AT                PIC 9(13)V9(2).                
               10  FILLER                        PIC X(40).                     
           05  DETAIL-RECORD-8-AREA REDEFINES DETAIL-RECORD-1.                  
               10  FI-COM-FIR-YR-AT              PIC 9(13)V9(2).                
               10  FI-COM-TRL-YR-AT              PIC 9(13)V9(2).                
               10  SLS-CRG-FIR-YR-AT             PIC 9(13)V9(2).                
               10  SLS-CRG-TRL-YR-AT             PIC 9(13)V9(2).                
               10  ISMT-FI-COM-RMN-AT            PIC 9(13)V9(2).                
               10  ISMT-END-IVT-CT               PIC 9(3)V9(6).                 
               10  ISMT-IVT-RMN-CT               PIC 9(3)V9(6).                 
               10  ISMT-SLS-CRG-RMN              PIC 9(13)V9(2).                
               10  GRF-GROSS-AT                  PIC 9(13)V9(2).                
               10  TOTAL-FEE-AT                  PIC 9(7)V9(2).                 
               10  INVESTMENT-MADE-CT            PIC 9(3)V9(6).                 
               10  FILLER                        PIC X(13).                     
           05  DETAIL-RECORD-9-AREA REDEFINES DETAIL-RECORD-1.                  
               10  FUND-CODE                     PIC 9(7).                      
               10  ACCOUNT-NUMBER                PIC 9(11).                     
               10  FIRM-ACCOUNT-NBR              PIC X(20).                     
               10  SEC-ISS-ID                    PIC X(9).                      
               10  TICKER-SYMBOL-ID              PIC X(5).                      
               10  TA-MUTUAL-FUND-ID             PIC X(20).                     
               10  EXTL-OMB-MFA-NBR              PIC X(20).                     
               10  OMB-FIRM-ACCT-NBR             PIC X(20).                     
               10  FILLER                        PIC X(42).                     
       WORKING-STORAGE SECTION.
       PROCEDURE DIVISION.
        STOP RUN.