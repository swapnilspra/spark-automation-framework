       IDENTIFICATION DIVISION.
       PROGRAM-ID. DIVIDEND-REFRESHER.
	   AUTHOR. DST. MODIFIED BY SHREENI.
	   DATE WRITTEN. 07/01/2009.
	   DATE COMPILED. 07/01/2009.
       ENVIRONMENT DIVISION.
       CONFIGURATION SECTION.
       FILE-CONTROL.
	    SELECT DIVIDEND-REFRESHER ASSIGN TO "FNAME".
       DATA DIVISION.
       FILE SECTION.
       FD  DIVIDEND-REFRESHER.
       01  DIVIDEND-REFRESHER.                                                             
           05  RECORD-TYPE                       PIC X(3).                      
               88  CAP-GAIN-HEADER                   VALUE 'CGH'.               
               88  CAP-GAIN-REFRESHER                VALUE 'CGR'.               
               88  DIVIDEND-HEADER                   VALUE 'DVH'.               
               88  DIVIDEND-REFRESHER                VALUE 'DVR'.               
               88  FOREIGN-TAX-CREDIT-HEADER         VALUE 'FTH'.               
               88  FOREIGN-TAX-CREDIT-REFRESHER      VALUE 'FTR'.               
           05  SEQUENCE-NUMBER                   PIC 9(3).                      
           05  HEADER-RECORD-1.                                                 
               10  DISTRIBUTION-RECORD-DATE.                                    
                   15  DISTRIBUTION-RECORD-YYYY-DATE.                           
                       20  DISTRIBUTION-RECORD-CC-DATE                          
                                                 PIC 9(2).                      
                       20  DISTRIBUTION-RECORD-YY-DATE                          
                                                 PIC 9(2).                      
                   15  DISTRIBUTION-RECORD-MM-DATE                              
                                                 PIC 9(2).                      
                   15  DISTRIBUTION-RECORD-DD-DATE                              
                                                 PIC 9(2).                      
               10  DISTRIBUTION-PAYABLE-DATE.                                   
                   15  DISTRIBUTION-PAYABLE-YYYY-DATE.                          
                       20  DISTRIBUTION-PAYABLE-CC-DATE                         
                                                 PIC 9(2).                      
                       20  DISTRIBUTION-PAYABLE-YY-DATE                         
                                                 PIC 9(2).                      
                   15  DISTRIBUTION-PAYABLE-MM-DATE                             
                                                 PIC 9(2).                      
                   15  DISTRIBUTION-PAYABLE-DD-DATE                             
                                                 PIC 9(2).                      
               10  DISTRIBUTION-REIN-DATE.                                      
                   15  DISTRIBUTION-REIN-YYYY-DATE.                             
                       20  DISTRIBUTION-REIN-CC-DATE                            
                                                 PIC 9(2).                      
                       20  DISTRIBUTION-REIN-YY-DATE                            
                                                 PIC 9(2).                      
                   15  DISTRIBUTION-REIN-MM-DATE PIC 9(2).                      
                   15  DISTRIBUTION-REIN-DD-DATE PIC 9(2).                      
               10  CUSIP-NUMBER                  PIC X(9).                      
               10  FUND-CODE                     PIC 9(7).                      
               10  DISTRIBUTION-PER-SHARE-RATE   PIC 9(5)V9(10).                
               10  FUND-EXPENSE-RATE             PIC 9(5)V9(10).                
               10  DISTRIBUTION-TYPE-CODE        PIC X(1).                      
                   88  DISTRIBUTION-DIVIDEND         VALUE '0'.                 
                   88  DISTRIBUTION-CAP-GAIN-SHORT   VALUE '1'.                 
                   88  DISTRIBUTION-CAP-GAIN-LONG    VALUE '2'.                 
                   88  DISTRIBUTION-STOCK-SPLIT      VALUE '3'.                 
               10  REINVESTED-SHARES-PRICE       PIC 9(5)V9(4).                 
               10  US-WITHHOLDING-RATE           PIC 9(5)V9(10).                
               10  MF-SP-DIV-TYPE-CDE            PIC X(1).                      
                   88  DTR-TYPE-SP-DIV-NOT-APP       VALUE ' '.                 
                   88  DTR-TYPE-SP-DIV-1             VALUE 'X'.                 
                   88  DTR-TYPE-SP-DIV-2             VALUE 'P'.                 
               10  DTR-LTCG-TYPE-CDE             PIC X(1).                      
                   88  DTR-LTCG-TYPE-NOT-APP         VALUE '0'.                 
                   88  DTR-LTCG-TYPE-28-PCT          VALUE '1'.                 
                   88  DTR-LTCG-TYPE-20-PCT          VALUE '2'.                 
               10  FILLER                        PIC X(57).                     
           05  DETAIL-RECORD-1 REDEFINES HEADER-RECORD-1.                       
               10  FINS-ID                       PIC 9(7).                      
               10  DEALER-BRANCH-ID              PIC X(9).                      
               10  CUSTOMER-ACCOUNT-NUMBER       PIC X(20).                     
               10  ACCOUNT-NUMBER-CODE           PIC X(1).                      
                   88  ACCOUNT-NUMBER-FUND           VALUE 'F'.                 
                   88  ACCOUNT-NUMBER-EXTERNAL       VALUE 'E'.                 
               10  FUND-EXPENSES-AMOUNT          PIC 9(13)V9(2).                
               10  FUND-EXPENSE-ALLOCATION-CODE  PIC X(1).                      
                   88  FUND-EXPENSE-ALLOC-SINGLE     VALUE '0'.                 
                   88  FUND-EXPENSE-ALLOC-CUMULATIVE VALUE '1'.                 
               10  TAX-IDENTIFICATION-NUMBER     PIC 9(9).                      
               10  SS-CODE                       PIC 9(1).                      
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
               10  DIST-TYPE-CODE                PIC X(1).                      
                   88  DIST-TYPE-REINVEST            VALUE '0'.                 
                   88  DIST-TYPE-INDIV-CHECK         VALUE '1'.                 
                   88  DIST-TYPE-REINVEST-ANOTHER-FND                           
                                                     VALUE '2'.                 
                   88  DIST-TYPE-CASH-IN-LIEU        VALUE '3'.                 
                   88  DIST-TYPE-FOREIGN-TAX-CREDIT  VALUE '4'.                 
                   88  DIST-TYPE-ACH-CASH            VALUE '5'.                 
                   88  DIST-TYPE-COMB-CHECK          VALUE '6'.                 
                   88  DIST-TYPE-INDIV-WIRE          VALUE '7'.                 
                   88  DIST-TYPE-COMB-WIRE           VALUE '8'.                 
                   88  DIST-TYPE-AGGR-WIRE           VALUE '9'.                 
               10  REINVESTED-SHARES-COUNT       PIC 9(11)V9(4).                
               10  ACCOUNT-SHRS-AFTER-DIST-COUNT PIC 9(11)V9(4).                
               10  ISSUED-SHARES-COUNT           PIC 9(11)V9(4).                
               10  DISTRIBUTION-AMOUNT           PIC 9(13)V9(2).                
               10  DISTRIBUTION-SUBACCT-FEE-AMT  PIC 9(7)V9(2).                 
               10  DISTRIBUTION-OTHER-FEE-AMOUNT PIC 9(7)V9(2).                 
               10  TRUSTEE-NSCC-FIRM-NBR         PIC X(4).                      
               10  NSCC-TPA-FIRM-NBR             PIC X(4).                      
               10  FILLER                        PIC X(4).                      
           05  DETAIL-RECORD-2 REDEFINES HEADER-RECORD-1.                       
               10  NRA-WITHHOLDING-RATE          PIC V9(3).                     
               10  NRA-WITHHOLDING-AMOUNT        PIC 9(13)V9(2).                
               10  US-WITHHOLDING-AMOUNT         PIC 9(13)V9(2).                
               10  CUMULATIVE-DISCOUNT-NUMBER    PIC 9(9).                      
               10  DEALER-CONTROL-LEVEL-CODE     PIC X(1).                      
                   88  NON-NETWORKED-BROKER-ACCOUNT  VALUE '0'.                 
                   88  BROKER-CONTROL-IRA-MARGIN-ACCT                           
                                                     VALUE '1'.                 
                   88  CUSTOMER-NAME-BROKERAGE-ACCT  VALUE '2'.                 
                   88  BROKER-CONTROL-BROKERAGE-ACCT VALUE '3'.                 
                   88  FUND-CONTROLLED-BROKERAGE-ACCT                           
                                                     VALUE '4'.                 
               10  REPRESENTATIVE-NUMBER         PIC X(9).                      
               10  REPRESENTATIVE-NAME           PIC X(30).                     
               10  EXTERNAL-PLAN-ID              PIC X(9).                      
               10  FUND-FROM-TO                  PIC 9(7).                      
               10  WIRE-TRANS-GRP-NBR            PIC 9(7).                      
               10  BALANCE-INDICATOR             PIC X(1).                      
                   88  ADD-TO-BALANCE                VALUE 'A'.                 
                   88  SUB-FROM-BALANCE              VALUE 'S'.                 
                   88  NO-CHANGE                     VALUE 'N'.                 
               10  TRANSACTION-CODE              PIC 9(3).                      
               10  TRANSACTION-SUFFIX            PIC 9(3).                      
               10  VOL-TRAN-DESC                 PIC X(26).                     
               10  CUSTODIAN-ID                  PIC 9(7).                      
               10  THIRD-PRTY-ADMN-ID            PIC 9(7).                      
               10  FILLER                        PIC X(2).                      
           05  DETAIL-RECORD-3 REDEFINES HEADER-RECORD-1.                       
               10  CUR-INCOME-DIVIDENDS          PIC 9(13)V9(2).                
               10  CUR-LONG-TERM-CAP-GAINS       PIC 9(13)V9(2).                
               10  CUR-SHORT-TERM-CAP-GAINS      PIC 9(13)V9(2).                
               10  CUR-FOREIGN-WITHHOLDING       PIC 9(13)V9(2).                
               10  CUR-DIVIDEND-WITHHOLDING      PIC 9(13)V9(2).                
               10  CUR-FOREIGN-TAX-PAID-AMT      PIC 9(13)V9(2).                
               10  FINS-ACCOUNT-ID               PIC X(20).                     
               10  FINS-ACCOUNT-CDE              PIC X(1).                      
                   88  FINS-ACCOUNT-FUND             VALUE 'F'.                 
                   88  FINS-ACCOUNT-EXTERNAL         VALUE 'E'.                 
               10  CUR-MF-LTCG-28-PCT-AMT        PIC 9(13)V9(2).                
               10  SHR-ACCT-CLIENT-DEFINED-1-CDE PIC X(1).                      
               10  SHR-ACCT-CLIENT-DEFINED-2-CDE PIC X(1).                      
               10  SHR-ACCT-CLIENT-DEFINED-3-CDE PIC X(1).                      
               10  SHR-ACCT-CLIENT-DEFINED-4-CDE PIC X(1).                      
               10  SHR-ACCT-CLIENT-DEFINED-5-CDE PIC X(1).                      
               10  SHR-ACCT-CLIENT-DEFINED-6-CDE PIC X(1).                      
               10  SHR-ACCT-CLIENT-DEFINED-7-CDE PIC X(1).                      
               10  SHR-ACCT-CLIENT-DEFINED-8-CDE PIC X(1).                      
               10  ALPHA-CODE                    PIC X(10).                     
               10  FILLER                        PIC X(10).                     
           05  DETAIL-RECORD-4 REDEFINES HEADER-RECORD-1.                       
               10  PREV-INCOME-DIVIDENDS         PIC 9(13)V9(2).                
               10  PREV-LONG-TERM-CAP-GAINS      PIC 9(13)V9(2).                
               10  PREV-SHORT-TERM-CAP-GAINS     PIC 9(13)V9(2).                
               10  PREV-FOREIGN-WITHHOLDING      PIC 9(13)V9(2).                
               10  PREV-DIVIDEND-WITHHOLDING     PIC 9(13)V9(2).                
               10  PREV-FOREIGN-TAX-PAID-AMT     PIC 9(13)V9(2).                
               10  PREV-MF-LTCG-28-PCT-AMT       PIC 9(13)V9(2).                
               10  CUSTOMER-ACCOUNT-NUMBER       PIC X(20).                     
               10  CUSTOMER-ACCOUNT-CD           PIC X(1).                      
                   88  CUSTOMER-ACCOUNT-FUND         VALUE 'F'.                 
                   88  CUSTOMER-ACCOUNT-EXTERNAL     VALUE 'E'.                 
               10  SEC-ISS-ID                    PIC X(9).                      
               10  FILLER                        PIC X(19).                     
           05  DETAIL-RECORD-5-AREA REDEFINES HEADER-RECORD-1.                  
               10  MF-TRAN-1-ID                  PIC 9(11).                     
               10  MF-TRAN-2-ID                  PIC 9(11).                     
               10  DEALER-COMMISSION             PIC 9(13)V9(2).                
               10  UNDERWRITER-COMMISSION        PIC S9(13)V9(2)                
                    SIGN TRAILING SEPARATE.                                     
               10  FILLER                        PIC X(101).                    
           05  DETAIL-RECORD-6 REDEFINES HEADER-RECORD-1.                       
               10  FUND-CODE                     PIC 9(7).                      
               10  ACCOUNT-NUMBER                PIC 9(11).                     
               10  FIRM-ACCOUNT-NBR              PIC X(20).                     
               10  SEC-ISS-ID                    PIC X(9).                      
               10  TICKER-SYMBOL-ID              PIC X(5).                      
               10  TA-MUTUAL-FUND-ID             PIC X(20).                     
               10  EXTL-OMB-MFA-NBR              PIC X(20).                     
               10  OMB-FIRM-ACCT-NBR             PIC X(20).                     
               10  FILLER                        PIC X(42).  
		  05  DETAIL-RECORD-7 REDEFINES HEADER-RECORD-1.                       
               10  FUND-CODE                     PIC 9(7).                      
               10  MF-GI-PRD-EXTERNAL-ID         PIC X(20).                     
               10  OWNERSHIP-TYPE-CD             PIC X(1).                      
                   88  OWNERSHIP-TYPE-ORIG           VALUE 'O'.                 
               10  FID-ACCT-TYPE-CD              PIC X(1).                      
                   88  FID-ACCT-TYPE-ROTH            VALUE 'R'.                 
                   88  FID-ACCT-TYPE-FID             VALUE 'F'.                 
                   88  FID-ACCT-TYPE-NA              VALUE 'Z'.                 
               10  TRADE-DATE                    PIC X(10).                     
               10  CONFIRM-DATE                  PIC X(10).                     
               10  GROSS-AMOUNT                  PIC 9(13)V9(2).                
               10  FILLER                        PIC X(90).  			   
       WORKING-STORAGE SECTION.
       PROCEDURE DIVISION.
        STOP RUN.			   
