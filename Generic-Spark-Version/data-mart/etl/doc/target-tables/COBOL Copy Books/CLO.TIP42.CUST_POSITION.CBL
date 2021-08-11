       IDENTIFICATION DIVISION.
       PROGRAM-ID. FBP-BRANCH-POSITION.
	   AUTHOR. DST. MODIFIED BY SHREENI.
	   DATE WRITTEN. 07/01/2009.
	   DATE COMPILED. 07/01/2009.
       ENVIRONMENT DIVISION.
       CONFIGURATION SECTION.
       FILE-CONTROL.
	    SELECT CLO-CUSTOMER-POSITION ASSIGN TO "FNAME".
       DATA DIVISION.
       FILE SECTION.
       FD  CLO-CUSTOMER-POSITION.
       01  CLO-CUSTOMER-POSITION. 
		   05  CMR-BPR-ID2                       PIC 9(11).
           05  CMR-BPR-ID1                       PIC 9(11).
           05  CMR-NME-PRE-TXT                   PIC X(8). 
           05  CMR-FRST-NME                      PIC X(35).
           05  CMR-MDL-NME                       PIC X(31).
           05  CMR-SRH-MDL-INI                   PIC X(1). 
           05  CMR-CRP-LST-NME                   PIC X(35).
           05  CMR-NME-SFX-TXT                   PIC X(8). 
           05  CUSTOMER-CORPORATE-NM             PIC X(70).
           05  CMR-TYP-CDE                       PIC X(1). 
               88  CMR-TYP-INDIVIDUAL                VALUE 'I'.
               88  CMR-TYP-CORPORATION               VALUE 'C'.
               88  CMR-TYP-UNKNOWN                   VALUE 'U'.
           05  CMR-TAX-ID                        PIC 9(9). 
           05  CMR-TAX-ID-TYP-CDE                PIC X(1). 
               88  CMR-TAX-ID-TYP-SSN                VALUE 'S'.
               88  CMR-TAX-ID-TYP-EIN                VALUE 'E'.
               88  CMR-TAX-ID-TYP-ITIN               VALUE 'I'.
               88  CMR-TAX-ID-TYP-UNKNOWN            VALUE 'U'.
          05  CMR-GND-CDE                       PIC X(1).
              88  CMR-GND-FEMALE                    VALUE 'F'.
              88  CMR-GND-MALE                      VALUE 'M'.
              88  CMR-GND-NOT-APPL                  VALUE 'N'.
              88  CMR-GND-UNKNOWN                   VALUE 'U'.
          05  CMR-MRTL-STS-CDE                  PIC X(1).
              88  CMR-MRTL-STS-SINGLE               VALUE 'S'.
              88  CMR-MRTL-STS-MARRIED              VALUE 'M'.
              88  CMR-MRTL-STS-UNKNOWN              VALUE 'U'.
          05  CMR-BTH-DTE                       PIC X(10).    
          05  CMR-DCS-DTE                       PIC X(10).    
          05  CMR-CTZ-CDE                       PIC X(1).
              88  CMR-CTZ-US-CTZ                    VALUE 'C'.
              88  CMR-CTZ-NON-RESIDENT-ALIEN        VALUE 'N'.
              88  CMR-CTZ-RESIDENT-ALIEN            VALUE 'R'.
              88  CMR-CTZ-UNKNOWN                   VALUE 'U'.
          05  TAX-RSP-ST-CNY-CDE                PIC 9(7).
          05  ORR-FGN-TAX-RTE                   PIC 9(5)V9(10).
          05  LABEL-PTL-ADR-LIN-TXT1            PIC X(35).    
          05  LABEL-PTL-ADR-LIN-TXT2            PIC X(35).    
          05  LABEL-PTL-ADR-LIN-TXT3            PIC X(35).    
          05  LABEL-PTL-ADR-LIN-TXT4            PIC X(35).    
          05  LABEL-PTL-ADR-LIN-TXT5            PIC X(35).    
          05  LABEL-PTL-ADR-LIN-TXT6            PIC X(35).    
          05  PTL-ADR-LIN-TXT1                  PIC X(35).    
          05  PTL-ADR-LIN-TXT2                  PIC X(35).    
          05  PTL-ADR-LIN-TXT3                  PIC X(35).    
          05  PTL-ADR-LIN-TXT4                  PIC X(35).    
          05  PTL-ADR-LIN-TXT5                  PIC X(35).    
          05  PTL-ADR-LIN-TXT6                  PIC X(35).    
          05  PTL-ADR-CTY-NME                   PIC X(35).    
          05  GOV-UNIT-LNG-NME                  PIC X(35).    
          05  POSTAL-CDE                        PIC X(2).
          05  STATE-COUNTRY-CDE                 PIC 9(7).
          05  US-IRS-CDE                        PIC X(2).
          05  ZIP-DELVRY-OFC-CDE                PIC 9(5).
          05  ZIP-CODE-SUFFIX                   PIC 9(4).
          05  ZIP-DELVRY-SEQ-NBR                PIC 9(2).
          05  CMR-SND-ADDRESS-EXIST-CD          PIC X(1).
              88  CMR-SND-ADDRESS-EXIST-YES         VALUE 'Y'. 
              88  CMR-SND-ADDRESS-EXIST-NO          VALUE 'N'. 
          05  PHN-NBR-TYP-CDE                   PIC X(1). 
              88  PHN-NBR-TYP-FORMATTED             VALUE 'F'. 
              88  PHN-NBR-TYP-UNFORMATTED           VALUE 'U'. 
          05  PHN-NBR-TXT                       PIC X(20).
          05  FTPHN-NBR-ARE-CDE                 PIC 9(3). 
          05  FTPHN-NBR-LCL-NBR                 PIC 9(7). 
          05  PHN-NBR-ITL-CCN                   PIC X(11).
          05  EVE-FTPHN-NBR-ARE-CDE             PIC 9(3). 
          05  EVE-FTPHN-NBR-LCL-NBR             PIC 9(7). 
          05  EVE-PHN-NBR-ITL-CCN               PIC X(11).
          05  CMR-INFD-CSNT-CD                  PIC X(1). 
              88  CMR-INFD-CSNT-YES                 VALUE 'Y'. 
              88  CMR-INFD-CSNT-NO                  VALUE 'N'. 
              88  CMR-INFD-CSNT-PENDING             VALUE 'P'. 
          05  CMR-STM-EML-ADR-ID                PIC X(100).    
          05  FD-SPNSR-SHRT-NME                 PIC X(3). 
          05  EXT-CMR-ID                        PIC X(20).
          05  EXT-PRSPCT-ID                     PIC X(20).
          05  CMR-LST-LNK-LOR                   PIC X(10). 
          05  CMR-MNTR-VIP-CDE                  PIC X(1).  
              88  CMR-MNTR-VIP-YES                  VALUE 'Y'.  
              88  CMR-MNTR-VIP-NO                   VALUE 'N'.  
          05  CMR-FS-EEE-CDE                    PIC X(1).  
              88  CMR-FS-EEE-YES                    VALUE 'Y'.  
              88  CMR-FS-EEE-NO                     VALUE 'N'.  
          05  CMR-REC-NON-PRG                   PIC X(1).  
              88  CMR-REC-NON-PRG-YES               VALUE 'Y'.  
              88  CMR-REC-NON-PRG-NO                VALUE 'N'.  
          05  CMR-SEN-MKT-ML-CDE                PIC X(1).  
              88  CMR-SEN-MKT-ML-YES                VALUE 'Y'.  
              88  CMR-SEN-MKT-ML-NO                 VALUE 'N'.  
          05  CMR-CDF-CD1                       PIC X(1).  
          05  CMR-CDF-CD2                       PIC X(1).  
          05  CMR-CDF-CD3                       PIC X(1).  
          05  CMR-CDF-CD4                       PIC X(1).  
          05  CMR-CDF-CD5                       PIC X(1).  
          05  CMR-CDF-CD6                       PIC X(1).  
          05  CMR-CDF-CD7                       PIC X(1).  
          05  CMR-CDF-CD8                       PIC X(1).   
          05  CMR-CDF-TX1                       PIC X(20).  
          05  PTL-ADR-TYP-CDE                   PIC X(1).   
              88  PTL-ADR-TYP-FORMATTED             VALUE 'F'.  
              88  PTL-ADR-TYP-UNFORMATTED           VALUE 'U'.  
          05  TRANSMITTER-ID                    PIC X(8).   
          05  REFRESHER-TYPE-CDE                PIC X(2).   
          05  TO-CMR-BPR-ID2                    PIC 9(11).  
          05  TO-CMR-BPR-ID1                    PIC 9(11).  
          05  CMR-PROCESS-TYPE-CD               PIC X(1).   
              88  CMR-PROCESS-TYPE-ADD              VALUE 'A'.  
              88  CMR-PROCESS-TYPE-UPDATE           VALUE 'U'.  
              88  CMR-PROCESS-TYPE-DELETE           VALUE 'D'.  
          05  CMR-UDV-ELC-ADR-CD                PIC X(1).   
              88  CMR-UDV-ELC-ADR-YES               VALUE 'Y'.  
              88  CMR-UDV-ELC-ADR-NO                VALUE 'N'.  
          05  CSNT-STM-TYPE-INVESTOR-CD         PIC X(1).   
              88  CSNT-STM-TYPE-INVESTOR-YES        VALUE 'Y'.  
              88  CSNT-STM-TYPE-INVESTOR-NO         VALUE 'N'.  
          05  CSNT-STM-TYPE-DAILY-CD            PIC X(1).   
              88  CSNT-STM-TYPE-DAILY-YES           VALUE 'Y'.   
              88  CSNT-STM-TYPE-DAILY-NO            VALUE 'N'.   
          05  CSNT-STM-TYPE-COMPLIANCE-CD       PIC X(1).   
              88  CSNT-STM-TYPE-COMPLIANCE-YES      VALUE 'Y'.   
              88  CSNT-STM-TYPE-COMPLIANCE-NO       VALUE 'N'.   
          05  CSNT-STM-TYPE-PROXY-CD            PIC X(1).   
              88  CSNT-STM-TYPE-PROXY-YES           VALUE 'Y'.   
              88  CSNT-STM-TYPE-PROXY-NO            VALUE 'N'.   
          05  CSNT-STM-TYPE-TAX-CD              PIC X(1).   
              88  CSNT-STM-TYPE-TAX-YES             VALUE 'Y'.   
              88  CSNT-STM-TYPE-TAX-NO              VALUE 'N'.   
          05  CSNT-STM-TYPE-AVG-COST-CD         PIC X(1).   
              88  CSNT-STM-TYPE-AVG-COST-YES        VALUE 'Y'.   
              88  CSNT-STM-TYPE-AVG-COST-NO         VALUE 'N'.   
          05  CSNT-STM-TYPE-ANNUAL-CD           PIC X(1).   
              88  CSNT-STM-TYPE-ANNUAL-YES          VALUE 'Y'.   
              88  CSNT-STM-TYPE-ANNUAL-NO           VALUE 'N'.   
          05  CSNT-STM-TYPE-REP-CD              PIC X(1).   
              88  CSNT-STM-TYPE-REP-YES             VALUE 'Y'.   
              88  CSNT-STM-TYPE-REP-NO              VALUE 'N'.   
          05  CSNT-STM-TYPE-MARKETING-CD        PIC X(1).     
              88  CSNT-STM-TYPE-MARKETING-YES       VALUE 'Y'.
              88  CSNT-STM-TYPE-MARKETING-NO        VALUE 'N'.
          05  CSNT-STM-TYPE-PART-PER-CD         PIC X(1).     
              88  CSNT-STM-TYPE-PART-PER-YES        VALUE 'Y'.
              88  CSNT-STM-TYPE-PART-PER-NO         VALUE 'N'.
          05  CSNT-STM-TYPE-COUTESY-EML-CD      PIC X(1).     
              88  CSNT-STM-TYPE-COUTESY-EML-YES     VALUE 'Y'.
              88  CSNT-STM-TYPE-COUTESY-EML-NO      VALUE 'N'.
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
          05  CSNT-STM-TYPE-MARKETING-CD        PIC X(1).     
              88  CSNT-STM-TYPE-MARKETING-YES       VALUE 'Y'.
              88  CSNT-STM-TYPE-MARKETING-NO        VALUE 'N'.
          05  CSNT-STM-TYPE-PART-PER-CD         PIC X(1).     
              88  CSNT-STM-TYPE-PART-PER-YES        VALUE 'Y'.
              88  CSNT-STM-TYPE-PART-PER-NO         VALUE 'N'.
          05  CSNT-STM-TYPE-COUTESY-EML-CD      PIC X(1).     
              88  CSNT-STM-TYPE-COUTESY-EML-YES     VALUE 'Y'.
              88  CSNT-STM-TYPE-COUTESY-EML-NO      VALUE 'N'.
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
       WORKING-STORAGE SECTION.
       PROCEDURE DIVISION.
        STOP RUN.				   

			  