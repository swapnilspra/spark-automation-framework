       IDENTIFICATION DIVISION.
       PROGRAM-ID. CLO-ACCOUNT-ACTIVITY.
	   AUTHOR. DST. MODIFIED BY SHREENI.
	   DATE WRITTEN. 07/01/2009.
	   DATE COMPILED. 07/01/2009.
       ENVIRONMENT DIVISION.
       CONFIGURATION SECTION.
       FILE-CONTROL.
	    SELECT CLO-ACCOUNT-ACTIVITY ASSIGN TO "FNAME".
       DATA DIVISION.
       FILE SECTION.
       FD  CLO-ACCOUNT-ACTIVITY.
       01  CLO-ACCOUNT-ACTIVITY.                                                             
           05  FUND-CODE                         PIC 9(7).                      
           05  ACCOUNT-NUMBER                    PIC 9(11).                     
           05  CMR-BPR-ID2                       PIC 9(11).                     
           05  CMR-BPR-ID1                       PIC 9(11).                     
           05  FS-REG-TYP-REL-ABR                PIC X(4).                      
           05  VALIDATE-ACCOUNT-ID               PIC 9(1).                      
           05  CMR-NME-PRE-TXT                   PIC X(8).                      
           05  CMR-FRST-NME                      PIC X(35).                     
           05  CMR-MDL-NME                       PIC X(31).                     
           05  CMR-CRP-LST-NME                   PIC X(35).                     
           05  CMR-NME-SFX-TXT                   PIC X(8).                      
           05  CUSTOMER-CORPORATE-NM             PIC X(70).                     
           05  CMR-TAX-ID                        PIC 9(9).                      
           05  CMR-TAX-ID-TYP-CDE                PIC X(1).                      
               88  CMR-TAX-ID-TYP-SSN                VALUE 'S'.                 
               88  CMR-TAX-ID-TYP-EIN                VALUE 'E'.                 
               88  CMR-TAX-ID-TYP-ITIN               VALUE 'I'.                 
               88  CMR-TAX-ID-TYP-UNKNOWN            VALUE 'U'.                 
           05  PAR-ZIP-DELVRY-OFC                PIC 9(5).                      
           05  LO-REG-BPR-ID2                    PIC 9(11).                     
           05  LO-REG-BPR-ID1                    PIC 9(11).                     
           05  CLOR-TAX-RSP-PRTY                 PIC X(1).                      
               88  CLOR-TAX-RSP-PRTY-YES             VALUE 'Y'.                 
               88  CLOR-TAX-RSP-PRTY-NO              VALUE 'N'.                 
           05  FS-REG-TYP-SHT-NME                PIC X(10).                     
           05  FD-SPNSR-SHRT-NME                 PIC X(3).                      
           05  SOCIAL-CODE                       PIC 9(3).                      
           05  TRANSMITTER-ID                    PIC X(8).                      
           05  REFRESHER-TYPE-CDE                PIC X(2).                      
           05  CMR-PROCESS-TYPE-CD               PIC X(1).                      
               88  CMR-PROCESS-TYPE-ADD              VALUE 'A'.                 
               88  CMR-PROCESS-TYPE-UPDATE           VALUE 'U'.                 
               88  CMR-PROCESS-TYPE-DELETE           VALUE 'D'.                 
           05  FINCL-INST-BRCH-ID                PIC X(9).                      
           05  FINANCIAL-INST-ID                 PIC S9(7). 
           05  ULTIMATE-BENEFICIAL-OWNER         PIC X(1).                      
               88  UBO-OWNER                         VALUE 'O'.                 
               88  UBO-BOTH                          VALUE 'B'.                 
               88  UBO-CONTLLR                       VALUE 'C'.                 
               88  UBO-SPACE                         VALUE 'Z'.		   
           05  REF-FORMAT-CDE                    PIC S9(3)       COMP-3.        
               88  REF-FORMAT-TIP                    VALUE 000.                 
               88  REF-FORMAT-NSCC                   VALUE 001.                 
               88  REF-FORMAT-BOSS                   VALUE 002.                 
               88  REF-FORMAT-TIP-DIRECT             VALUE 003.                 
               88  REF-FORMAT-TIP-ORDER              VALUE 004.                 
               88  REF-FORMAT-TIP-GLOBAL             VALUE 099.                 
           05  REF-RQST-NBR                      PIC S9(7)       COMP-3.        
           05  RFHR-MASK-TIN-CD                  PIC X(1).                      
               88  RFHR-MASK-TIN-ALL-ZEROS           VALUE 'A'.                 
               88  RFHR-MASK-TIN-NONE                VALUE 'N'.                 
               88  RFHR-MASK-TIN-NA                  VALUE 'Z'.                 
           05  FILLER                            PIC X(60).  
       WORKING-STORAGE SECTION.
       PROCEDURE DIVISION.
        STOP RUN.			   
                   
