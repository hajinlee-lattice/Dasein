// ORM class for table 'Nutanix'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Tue May 26 07:59:18 PDT 2015
// For connector: org.apache.sqoop.manager.GenericJdbcManager
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import com.cloudera.sqoop.lib.JdbcWritableBridge;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.BooleanParser;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Nutanix extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  protected ResultSet __cur_result_set;
  private Long Nutanix_EventTable_Clean;
  public Long get_Nutanix_EventTable_Clean() {
    return Nutanix_EventTable_Clean;
  }
  public void set_Nutanix_EventTable_Clean(Long Nutanix_EventTable_Clean) {
    this.Nutanix_EventTable_Clean = Nutanix_EventTable_Clean;
  }
  public Nutanix with_Nutanix_EventTable_Clean(Long Nutanix_EventTable_Clean) {
    this.Nutanix_EventTable_Clean = Nutanix_EventTable_Clean;
    return this;
  }
  private String LeadID;
  public String get_LeadID() {
    return LeadID;
  }
  public void set_LeadID(String LeadID) {
    this.LeadID = LeadID;
  }
  public Nutanix with_LeadID(String LeadID) {
    this.LeadID = LeadID;
    return this;
  }
  private String CustomerID;
  public String get_CustomerID() {
    return CustomerID;
  }
  public void set_CustomerID(String CustomerID) {
    this.CustomerID = CustomerID;
  }
  public Nutanix with_CustomerID(String CustomerID) {
    this.CustomerID = CustomerID;
    return this;
  }
  private Long PeriodID;
  public Long get_PeriodID() {
    return PeriodID;
  }
  public void set_PeriodID(Long PeriodID) {
    this.PeriodID = PeriodID;
  }
  public Nutanix with_PeriodID(Long PeriodID) {
    this.PeriodID = PeriodID;
    return this;
  }
  private Long P1_Target;
  public Long get_P1_Target() {
    return P1_Target;
  }
  public void set_P1_Target(Long P1_Target) {
    this.P1_Target = P1_Target;
  }
  public Nutanix with_P1_Target(Long P1_Target) {
    this.P1_Target = P1_Target;
    return this;
  }
  private Long P1_TargetTraining;
  public Long get_P1_TargetTraining() {
    return P1_TargetTraining;
  }
  public void set_P1_TargetTraining(Long P1_TargetTraining) {
    this.P1_TargetTraining = P1_TargetTraining;
  }
  public Nutanix with_P1_TargetTraining(Long P1_TargetTraining) {
    this.P1_TargetTraining = P1_TargetTraining;
    return this;
  }
  private String P1_Event;
  public String get_P1_Event() {
    return P1_Event;
  }
  public void set_P1_Event(String P1_Event) {
    this.P1_Event = P1_Event;
  }
  public Nutanix with_P1_Event(String P1_Event) {
    this.P1_Event = P1_Event;
    return this;
  }
  private String Company;
  public String get_Company() {
    return Company;
  }
  public void set_Company(String Company) {
    this.Company = Company;
  }
  public Nutanix with_Company(String Company) {
    this.Company = Company;
    return this;
  }
  private String Email;
  public String get_Email() {
    return Email;
  }
  public void set_Email(String Email) {
    this.Email = Email;
  }
  public Nutanix with_Email(String Email) {
    this.Email = Email;
    return this;
  }
  private String Domain;
  public String get_Domain() {
    return Domain;
  }
  public void set_Domain(String Domain) {
    this.Domain = Domain;
  }
  public Nutanix with_Domain(String Domain) {
    this.Domain = Domain;
    return this;
  }
  private Long AwardYear;
  public Long get_AwardYear() {
    return AwardYear;
  }
  public void set_AwardYear(Long AwardYear) {
    this.AwardYear = AwardYear;
  }
  public Nutanix with_AwardYear(Long AwardYear) {
    this.AwardYear = AwardYear;
    return this;
  }
  private String BankruptcyFiled;
  public String get_BankruptcyFiled() {
    return BankruptcyFiled;
  }
  public void set_BankruptcyFiled(String BankruptcyFiled) {
    this.BankruptcyFiled = BankruptcyFiled;
  }
  public Nutanix with_BankruptcyFiled(String BankruptcyFiled) {
    this.BankruptcyFiled = BankruptcyFiled;
    return this;
  }
  private Double BusinessAnnualSalesAbs;
  public Double get_BusinessAnnualSalesAbs() {
    return BusinessAnnualSalesAbs;
  }
  public void set_BusinessAnnualSalesAbs(Double BusinessAnnualSalesAbs) {
    this.BusinessAnnualSalesAbs = BusinessAnnualSalesAbs;
  }
  public Nutanix with_BusinessAnnualSalesAbs(Double BusinessAnnualSalesAbs) {
    this.BusinessAnnualSalesAbs = BusinessAnnualSalesAbs;
    return this;
  }
  private String BusinessAssets;
  public String get_BusinessAssets() {
    return BusinessAssets;
  }
  public void set_BusinessAssets(String BusinessAssets) {
    this.BusinessAssets = BusinessAssets;
  }
  public Nutanix with_BusinessAssets(String BusinessAssets) {
    this.BusinessAssets = BusinessAssets;
    return this;
  }
  private String BusinessECommerceSite;
  public String get_BusinessECommerceSite() {
    return BusinessECommerceSite;
  }
  public void set_BusinessECommerceSite(String BusinessECommerceSite) {
    this.BusinessECommerceSite = BusinessECommerceSite;
  }
  public Nutanix with_BusinessECommerceSite(String BusinessECommerceSite) {
    this.BusinessECommerceSite = BusinessECommerceSite;
    return this;
  }
  private String BusinessEntityType;
  public String get_BusinessEntityType() {
    return BusinessEntityType;
  }
  public void set_BusinessEntityType(String BusinessEntityType) {
    this.BusinessEntityType = BusinessEntityType;
  }
  public Nutanix with_BusinessEntityType(String BusinessEntityType) {
    this.BusinessEntityType = BusinessEntityType;
    return this;
  }
  private Double BusinessEstablishedYear;
  public Double get_BusinessEstablishedYear() {
    return BusinessEstablishedYear;
  }
  public void set_BusinessEstablishedYear(Double BusinessEstablishedYear) {
    this.BusinessEstablishedYear = BusinessEstablishedYear;
  }
  public Nutanix with_BusinessEstablishedYear(Double BusinessEstablishedYear) {
    this.BusinessEstablishedYear = BusinessEstablishedYear;
    return this;
  }
  private Double BusinessEstimatedAnnualSales_k;
  public Double get_BusinessEstimatedAnnualSales_k() {
    return BusinessEstimatedAnnualSales_k;
  }
  public void set_BusinessEstimatedAnnualSales_k(Double BusinessEstimatedAnnualSales_k) {
    this.BusinessEstimatedAnnualSales_k = BusinessEstimatedAnnualSales_k;
  }
  public Nutanix with_BusinessEstimatedAnnualSales_k(Double BusinessEstimatedAnnualSales_k) {
    this.BusinessEstimatedAnnualSales_k = BusinessEstimatedAnnualSales_k;
    return this;
  }
  private Double BusinessEstimatedEmployees;
  public Double get_BusinessEstimatedEmployees() {
    return BusinessEstimatedEmployees;
  }
  public void set_BusinessEstimatedEmployees(Double BusinessEstimatedEmployees) {
    this.BusinessEstimatedEmployees = BusinessEstimatedEmployees;
  }
  public Nutanix with_BusinessEstimatedEmployees(Double BusinessEstimatedEmployees) {
    this.BusinessEstimatedEmployees = BusinessEstimatedEmployees;
    return this;
  }
  private Double BusinessFirmographicsParentEmployees;
  public Double get_BusinessFirmographicsParentEmployees() {
    return BusinessFirmographicsParentEmployees;
  }
  public void set_BusinessFirmographicsParentEmployees(Double BusinessFirmographicsParentEmployees) {
    this.BusinessFirmographicsParentEmployees = BusinessFirmographicsParentEmployees;
  }
  public Nutanix with_BusinessFirmographicsParentEmployees(Double BusinessFirmographicsParentEmployees) {
    this.BusinessFirmographicsParentEmployees = BusinessFirmographicsParentEmployees;
    return this;
  }
  private Double BusinessFirmographicsParentRevenue;
  public Double get_BusinessFirmographicsParentRevenue() {
    return BusinessFirmographicsParentRevenue;
  }
  public void set_BusinessFirmographicsParentRevenue(Double BusinessFirmographicsParentRevenue) {
    this.BusinessFirmographicsParentRevenue = BusinessFirmographicsParentRevenue;
  }
  public Nutanix with_BusinessFirmographicsParentRevenue(Double BusinessFirmographicsParentRevenue) {
    this.BusinessFirmographicsParentRevenue = BusinessFirmographicsParentRevenue;
    return this;
  }
  private String BusinessIndustrySector;
  public String get_BusinessIndustrySector() {
    return BusinessIndustrySector;
  }
  public void set_BusinessIndustrySector(String BusinessIndustrySector) {
    this.BusinessIndustrySector = BusinessIndustrySector;
  }
  public Nutanix with_BusinessIndustrySector(String BusinessIndustrySector) {
    this.BusinessIndustrySector = BusinessIndustrySector;
    return this;
  }
  private Double BusinessRetirementParticipants;
  public Double get_BusinessRetirementParticipants() {
    return BusinessRetirementParticipants;
  }
  public void set_BusinessRetirementParticipants(Double BusinessRetirementParticipants) {
    this.BusinessRetirementParticipants = BusinessRetirementParticipants;
  }
  public Nutanix with_BusinessRetirementParticipants(Double BusinessRetirementParticipants) {
    this.BusinessRetirementParticipants = BusinessRetirementParticipants;
    return this;
  }
  private String BusinessSocialPresence;
  public String get_BusinessSocialPresence() {
    return BusinessSocialPresence;
  }
  public void set_BusinessSocialPresence(String BusinessSocialPresence) {
    this.BusinessSocialPresence = BusinessSocialPresence;
  }
  public Nutanix with_BusinessSocialPresence(String BusinessSocialPresence) {
    this.BusinessSocialPresence = BusinessSocialPresence;
    return this;
  }
  private Double BusinessUrlNumPages;
  public Double get_BusinessUrlNumPages() {
    return BusinessUrlNumPages;
  }
  public void set_BusinessUrlNumPages(Double BusinessUrlNumPages) {
    this.BusinessUrlNumPages = BusinessUrlNumPages;
  }
  public Nutanix with_BusinessUrlNumPages(Double BusinessUrlNumPages) {
    this.BusinessUrlNumPages = BusinessUrlNumPages;
    return this;
  }
  private String BusinessType;
  public String get_BusinessType() {
    return BusinessType;
  }
  public void set_BusinessType(String BusinessType) {
    this.BusinessType = BusinessType;
  }
  public Nutanix with_BusinessType(String BusinessType) {
    this.BusinessType = BusinessType;
    return this;
  }
  private String BusinessVCFunded;
  public String get_BusinessVCFunded() {
    return BusinessVCFunded;
  }
  public void set_BusinessVCFunded(String BusinessVCFunded) {
    this.BusinessVCFunded = BusinessVCFunded;
  }
  public Nutanix with_BusinessVCFunded(String BusinessVCFunded) {
    this.BusinessVCFunded = BusinessVCFunded;
    return this;
  }
  private String DerogatoryIndicator;
  public String get_DerogatoryIndicator() {
    return DerogatoryIndicator;
  }
  public void set_DerogatoryIndicator(String DerogatoryIndicator) {
    this.DerogatoryIndicator = DerogatoryIndicator;
  }
  public Nutanix with_DerogatoryIndicator(String DerogatoryIndicator) {
    this.DerogatoryIndicator = DerogatoryIndicator;
    return this;
  }
  private String ExperianCreditRating;
  public String get_ExperianCreditRating() {
    return ExperianCreditRating;
  }
  public void set_ExperianCreditRating(String ExperianCreditRating) {
    this.ExperianCreditRating = ExperianCreditRating;
  }
  public Nutanix with_ExperianCreditRating(String ExperianCreditRating) {
    this.ExperianCreditRating = ExperianCreditRating;
    return this;
  }
  private String FundingAgency;
  public String get_FundingAgency() {
    return FundingAgency;
  }
  public void set_FundingAgency(String FundingAgency) {
    this.FundingAgency = FundingAgency;
  }
  public Nutanix with_FundingAgency(String FundingAgency) {
    this.FundingAgency = FundingAgency;
    return this;
  }
  private Double FundingAmount;
  public Double get_FundingAmount() {
    return FundingAmount;
  }
  public void set_FundingAmount(Double FundingAmount) {
    this.FundingAmount = FundingAmount;
  }
  public Nutanix with_FundingAmount(Double FundingAmount) {
    this.FundingAmount = FundingAmount;
    return this;
  }
  private Double FundingAwardAmount;
  public Double get_FundingAwardAmount() {
    return FundingAwardAmount;
  }
  public void set_FundingAwardAmount(Double FundingAwardAmount) {
    this.FundingAwardAmount = FundingAwardAmount;
  }
  public Nutanix with_FundingAwardAmount(Double FundingAwardAmount) {
    this.FundingAwardAmount = FundingAwardAmount;
    return this;
  }
  private Double FundingFinanceRound;
  public Double get_FundingFinanceRound() {
    return FundingFinanceRound;
  }
  public void set_FundingFinanceRound(Double FundingFinanceRound) {
    this.FundingFinanceRound = FundingFinanceRound;
  }
  public Nutanix with_FundingFinanceRound(Double FundingFinanceRound) {
    this.FundingFinanceRound = FundingFinanceRound;
    return this;
  }
  private Double FundingFiscalQuarter;
  public Double get_FundingFiscalQuarter() {
    return FundingFiscalQuarter;
  }
  public void set_FundingFiscalQuarter(Double FundingFiscalQuarter) {
    this.FundingFiscalQuarter = FundingFiscalQuarter;
  }
  public Nutanix with_FundingFiscalQuarter(Double FundingFiscalQuarter) {
    this.FundingFiscalQuarter = FundingFiscalQuarter;
    return this;
  }
  private Double FundingFiscalYear;
  public Double get_FundingFiscalYear() {
    return FundingFiscalYear;
  }
  public void set_FundingFiscalYear(Double FundingFiscalYear) {
    this.FundingFiscalYear = FundingFiscalYear;
  }
  public Nutanix with_FundingFiscalYear(Double FundingFiscalYear) {
    this.FundingFiscalYear = FundingFiscalYear;
    return this;
  }
  private Double FundingReceived;
  public Double get_FundingReceived() {
    return FundingReceived;
  }
  public void set_FundingReceived(Double FundingReceived) {
    this.FundingReceived = FundingReceived;
  }
  public Nutanix with_FundingReceived(Double FundingReceived) {
    this.FundingReceived = FundingReceived;
    return this;
  }
  private String FundingStage;
  public String get_FundingStage() {
    return FundingStage;
  }
  public void set_FundingStage(String FundingStage) {
    this.FundingStage = FundingStage;
  }
  public Nutanix with_FundingStage(String FundingStage) {
    this.FundingStage = FundingStage;
    return this;
  }
  private Double Intelliscore;
  public Double get_Intelliscore() {
    return Intelliscore;
  }
  public void set_Intelliscore(Double Intelliscore) {
    this.Intelliscore = Intelliscore;
  }
  public Nutanix with_Intelliscore(Double Intelliscore) {
    this.Intelliscore = Intelliscore;
    return this;
  }
  private Double JobsRecentJobs;
  public Double get_JobsRecentJobs() {
    return JobsRecentJobs;
  }
  public void set_JobsRecentJobs(Double JobsRecentJobs) {
    this.JobsRecentJobs = JobsRecentJobs;
  }
  public Nutanix with_JobsRecentJobs(Double JobsRecentJobs) {
    this.JobsRecentJobs = JobsRecentJobs;
    return this;
  }
  private String JobsTrendString;
  public String get_JobsTrendString() {
    return JobsTrendString;
  }
  public void set_JobsTrendString(String JobsTrendString) {
    this.JobsTrendString = JobsTrendString;
  }
  public Nutanix with_JobsTrendString(String JobsTrendString) {
    this.JobsTrendString = JobsTrendString;
    return this;
  }
  private String ModelAction;
  public String get_ModelAction() {
    return ModelAction;
  }
  public void set_ModelAction(String ModelAction) {
    this.ModelAction = ModelAction;
  }
  public Nutanix with_ModelAction(String ModelAction) {
    this.ModelAction = ModelAction;
    return this;
  }
  private Double PercentileModel;
  public Double get_PercentileModel() {
    return PercentileModel;
  }
  public void set_PercentileModel(Double PercentileModel) {
    this.PercentileModel = PercentileModel;
  }
  public Nutanix with_PercentileModel(Double PercentileModel) {
    this.PercentileModel = PercentileModel;
    return this;
  }
  private Double RetirementAssetsEOY;
  public Double get_RetirementAssetsEOY() {
    return RetirementAssetsEOY;
  }
  public void set_RetirementAssetsEOY(Double RetirementAssetsEOY) {
    this.RetirementAssetsEOY = RetirementAssetsEOY;
  }
  public Nutanix with_RetirementAssetsEOY(Double RetirementAssetsEOY) {
    this.RetirementAssetsEOY = RetirementAssetsEOY;
    return this;
  }
  private Double RetirementAssetsYOY;
  public Double get_RetirementAssetsYOY() {
    return RetirementAssetsYOY;
  }
  public void set_RetirementAssetsYOY(Double RetirementAssetsYOY) {
    this.RetirementAssetsYOY = RetirementAssetsYOY;
  }
  public Nutanix with_RetirementAssetsYOY(Double RetirementAssetsYOY) {
    this.RetirementAssetsYOY = RetirementAssetsYOY;
    return this;
  }
  private Double TotalParticipantsSOY;
  public Double get_TotalParticipantsSOY() {
    return TotalParticipantsSOY;
  }
  public void set_TotalParticipantsSOY(Double TotalParticipantsSOY) {
    this.TotalParticipantsSOY = TotalParticipantsSOY;
  }
  public Nutanix with_TotalParticipantsSOY(Double TotalParticipantsSOY) {
    this.TotalParticipantsSOY = TotalParticipantsSOY;
    return this;
  }
  private Double UCCFilings;
  public Double get_UCCFilings() {
    return UCCFilings;
  }
  public void set_UCCFilings(Double UCCFilings) {
    this.UCCFilings = UCCFilings;
  }
  public Nutanix with_UCCFilings(Double UCCFilings) {
    this.UCCFilings = UCCFilings;
    return this;
  }
  private String UCCFilingsPresent;
  public String get_UCCFilingsPresent() {
    return UCCFilingsPresent;
  }
  public void set_UCCFilingsPresent(String UCCFilingsPresent) {
    this.UCCFilingsPresent = UCCFilingsPresent;
  }
  public Nutanix with_UCCFilingsPresent(String UCCFilingsPresent) {
    this.UCCFilingsPresent = UCCFilingsPresent;
    return this;
  }
  private String Years_in_Business_Code;
  public String get_Years_in_Business_Code() {
    return Years_in_Business_Code;
  }
  public void set_Years_in_Business_Code(String Years_in_Business_Code) {
    this.Years_in_Business_Code = Years_in_Business_Code;
  }
  public Nutanix with_Years_in_Business_Code(String Years_in_Business_Code) {
    this.Years_in_Business_Code = Years_in_Business_Code;
    return this;
  }
  private String Non_Profit_Indicator;
  public String get_Non_Profit_Indicator() {
    return Non_Profit_Indicator;
  }
  public void set_Non_Profit_Indicator(String Non_Profit_Indicator) {
    this.Non_Profit_Indicator = Non_Profit_Indicator;
  }
  public Nutanix with_Non_Profit_Indicator(String Non_Profit_Indicator) {
    this.Non_Profit_Indicator = Non_Profit_Indicator;
    return this;
  }
  private String PD_DA_AwardCategory;
  public String get_PD_DA_AwardCategory() {
    return PD_DA_AwardCategory;
  }
  public void set_PD_DA_AwardCategory(String PD_DA_AwardCategory) {
    this.PD_DA_AwardCategory = PD_DA_AwardCategory;
  }
  public Nutanix with_PD_DA_AwardCategory(String PD_DA_AwardCategory) {
    this.PD_DA_AwardCategory = PD_DA_AwardCategory;
    return this;
  }
  private String PD_DA_JobTitle;
  public String get_PD_DA_JobTitle() {
    return PD_DA_JobTitle;
  }
  public void set_PD_DA_JobTitle(String PD_DA_JobTitle) {
    this.PD_DA_JobTitle = PD_DA_JobTitle;
  }
  public Nutanix with_PD_DA_JobTitle(String PD_DA_JobTitle) {
    this.PD_DA_JobTitle = PD_DA_JobTitle;
    return this;
  }
  private String PD_DA_LastSocialActivity_Units;
  public String get_PD_DA_LastSocialActivity_Units() {
    return PD_DA_LastSocialActivity_Units;
  }
  public void set_PD_DA_LastSocialActivity_Units(String PD_DA_LastSocialActivity_Units) {
    this.PD_DA_LastSocialActivity_Units = PD_DA_LastSocialActivity_Units;
  }
  public Nutanix with_PD_DA_LastSocialActivity_Units(String PD_DA_LastSocialActivity_Units) {
    this.PD_DA_LastSocialActivity_Units = PD_DA_LastSocialActivity_Units;
    return this;
  }
  private Double PD_DA_MonthsPatentGranted;
  public Double get_PD_DA_MonthsPatentGranted() {
    return PD_DA_MonthsPatentGranted;
  }
  public void set_PD_DA_MonthsPatentGranted(Double PD_DA_MonthsPatentGranted) {
    this.PD_DA_MonthsPatentGranted = PD_DA_MonthsPatentGranted;
  }
  public Nutanix with_PD_DA_MonthsPatentGranted(Double PD_DA_MonthsPatentGranted) {
    this.PD_DA_MonthsPatentGranted = PD_DA_MonthsPatentGranted;
    return this;
  }
  private Double PD_DA_MonthsSinceFundAwardDate;
  public Double get_PD_DA_MonthsSinceFundAwardDate() {
    return PD_DA_MonthsSinceFundAwardDate;
  }
  public void set_PD_DA_MonthsSinceFundAwardDate(Double PD_DA_MonthsSinceFundAwardDate) {
    this.PD_DA_MonthsSinceFundAwardDate = PD_DA_MonthsSinceFundAwardDate;
  }
  public Nutanix with_PD_DA_MonthsSinceFundAwardDate(Double PD_DA_MonthsSinceFundAwardDate) {
    this.PD_DA_MonthsSinceFundAwardDate = PD_DA_MonthsSinceFundAwardDate;
    return this;
  }
  private String PD_DA_PrimarySIC1;
  public String get_PD_DA_PrimarySIC1() {
    return PD_DA_PrimarySIC1;
  }
  public void set_PD_DA_PrimarySIC1(String PD_DA_PrimarySIC1) {
    this.PD_DA_PrimarySIC1 = PD_DA_PrimarySIC1;
  }
  public Nutanix with_PD_DA_PrimarySIC1(String PD_DA_PrimarySIC1) {
    this.PD_DA_PrimarySIC1 = PD_DA_PrimarySIC1;
    return this;
  }
  private String Industry;
  public String get_Industry() {
    return Industry;
  }
  public void set_Industry(String Industry) {
    this.Industry = Industry;
  }
  public Nutanix with_Industry(String Industry) {
    this.Industry = Industry;
    return this;
  }
  private String LeadSource;
  public String get_LeadSource() {
    return LeadSource;
  }
  public void set_LeadSource(String LeadSource) {
    this.LeadSource = LeadSource;
  }
  public Nutanix with_LeadSource(String LeadSource) {
    this.LeadSource = LeadSource;
    return this;
  }
  private Double AnnualRevenue;
  public Double get_AnnualRevenue() {
    return AnnualRevenue;
  }
  public void set_AnnualRevenue(Double AnnualRevenue) {
    this.AnnualRevenue = AnnualRevenue;
  }
  public Nutanix with_AnnualRevenue(Double AnnualRevenue) {
    this.AnnualRevenue = AnnualRevenue;
    return this;
  }
  private Double NumberOfEmployees;
  public Double get_NumberOfEmployees() {
    return NumberOfEmployees;
  }
  public void set_NumberOfEmployees(Double NumberOfEmployees) {
    this.NumberOfEmployees = NumberOfEmployees;
  }
  public Nutanix with_NumberOfEmployees(Double NumberOfEmployees) {
    this.NumberOfEmployees = NumberOfEmployees;
    return this;
  }
  private Double Alexa_MonthsSinceOnline;
  public Double get_Alexa_MonthsSinceOnline() {
    return Alexa_MonthsSinceOnline;
  }
  public void set_Alexa_MonthsSinceOnline(Double Alexa_MonthsSinceOnline) {
    this.Alexa_MonthsSinceOnline = Alexa_MonthsSinceOnline;
  }
  public Nutanix with_Alexa_MonthsSinceOnline(Double Alexa_MonthsSinceOnline) {
    this.Alexa_MonthsSinceOnline = Alexa_MonthsSinceOnline;
    return this;
  }
  private Double Alexa_Rank;
  public Double get_Alexa_Rank() {
    return Alexa_Rank;
  }
  public void set_Alexa_Rank(Double Alexa_Rank) {
    this.Alexa_Rank = Alexa_Rank;
  }
  public Nutanix with_Alexa_Rank(Double Alexa_Rank) {
    this.Alexa_Rank = Alexa_Rank;
    return this;
  }
  private Double Alexa_ReachPerMillion;
  public Double get_Alexa_ReachPerMillion() {
    return Alexa_ReachPerMillion;
  }
  public void set_Alexa_ReachPerMillion(Double Alexa_ReachPerMillion) {
    this.Alexa_ReachPerMillion = Alexa_ReachPerMillion;
  }
  public Nutanix with_Alexa_ReachPerMillion(Double Alexa_ReachPerMillion) {
    this.Alexa_ReachPerMillion = Alexa_ReachPerMillion;
    return this;
  }
  private Double Alexa_ViewsPerMillion;
  public Double get_Alexa_ViewsPerMillion() {
    return Alexa_ViewsPerMillion;
  }
  public void set_Alexa_ViewsPerMillion(Double Alexa_ViewsPerMillion) {
    this.Alexa_ViewsPerMillion = Alexa_ViewsPerMillion;
  }
  public Nutanix with_Alexa_ViewsPerMillion(Double Alexa_ViewsPerMillion) {
    this.Alexa_ViewsPerMillion = Alexa_ViewsPerMillion;
    return this;
  }
  private Double Alexa_ViewsPerUser;
  public Double get_Alexa_ViewsPerUser() {
    return Alexa_ViewsPerUser;
  }
  public void set_Alexa_ViewsPerUser(Double Alexa_ViewsPerUser) {
    this.Alexa_ViewsPerUser = Alexa_ViewsPerUser;
  }
  public Nutanix with_Alexa_ViewsPerUser(Double Alexa_ViewsPerUser) {
    this.Alexa_ViewsPerUser = Alexa_ViewsPerUser;
    return this;
  }
  private Double BW_TechTags_Cnt;
  public Double get_BW_TechTags_Cnt() {
    return BW_TechTags_Cnt;
  }
  public void set_BW_TechTags_Cnt(Double BW_TechTags_Cnt) {
    this.BW_TechTags_Cnt = BW_TechTags_Cnt;
  }
  public Nutanix with_BW_TechTags_Cnt(Double BW_TechTags_Cnt) {
    this.BW_TechTags_Cnt = BW_TechTags_Cnt;
    return this;
  }
  private Double BW_TotalTech_Cnt;
  public Double get_BW_TotalTech_Cnt() {
    return BW_TotalTech_Cnt;
  }
  public void set_BW_TotalTech_Cnt(Double BW_TotalTech_Cnt) {
    this.BW_TotalTech_Cnt = BW_TotalTech_Cnt;
  }
  public Nutanix with_BW_TotalTech_Cnt(Double BW_TotalTech_Cnt) {
    this.BW_TotalTech_Cnt = BW_TotalTech_Cnt;
    return this;
  }
  private Double BW_ads;
  public Double get_BW_ads() {
    return BW_ads;
  }
  public void set_BW_ads(Double BW_ads) {
    this.BW_ads = BW_ads;
  }
  public Nutanix with_BW_ads(Double BW_ads) {
    this.BW_ads = BW_ads;
    return this;
  }
  private Double BW_analytics;
  public Double get_BW_analytics() {
    return BW_analytics;
  }
  public void set_BW_analytics(Double BW_analytics) {
    this.BW_analytics = BW_analytics;
  }
  public Nutanix with_BW_analytics(Double BW_analytics) {
    this.BW_analytics = BW_analytics;
    return this;
  }
  private Double BW_cdn;
  public Double get_BW_cdn() {
    return BW_cdn;
  }
  public void set_BW_cdn(Double BW_cdn) {
    this.BW_cdn = BW_cdn;
  }
  public Nutanix with_BW_cdn(Double BW_cdn) {
    this.BW_cdn = BW_cdn;
    return this;
  }
  private Double BW_cdns;
  public Double get_BW_cdns() {
    return BW_cdns;
  }
  public void set_BW_cdns(Double BW_cdns) {
    this.BW_cdns = BW_cdns;
  }
  public Nutanix with_BW_cdns(Double BW_cdns) {
    this.BW_cdns = BW_cdns;
    return this;
  }
  private Double BW_cms;
  public Double get_BW_cms() {
    return BW_cms;
  }
  public void set_BW_cms(Double BW_cms) {
    this.BW_cms = BW_cms;
  }
  public Nutanix with_BW_cms(Double BW_cms) {
    this.BW_cms = BW_cms;
    return this;
  }
  private Double BW_docinfo;
  public Double get_BW_docinfo() {
    return BW_docinfo;
  }
  public void set_BW_docinfo(Double BW_docinfo) {
    this.BW_docinfo = BW_docinfo;
  }
  public Nutanix with_BW_docinfo(Double BW_docinfo) {
    this.BW_docinfo = BW_docinfo;
    return this;
  }
  private Double BW_encoding;
  public Double get_BW_encoding() {
    return BW_encoding;
  }
  public void set_BW_encoding(Double BW_encoding) {
    this.BW_encoding = BW_encoding;
  }
  public Nutanix with_BW_encoding(Double BW_encoding) {
    this.BW_encoding = BW_encoding;
    return this;
  }
  private Double BW_feeds;
  public Double get_BW_feeds() {
    return BW_feeds;
  }
  public void set_BW_feeds(Double BW_feeds) {
    this.BW_feeds = BW_feeds;
  }
  public Nutanix with_BW_feeds(Double BW_feeds) {
    this.BW_feeds = BW_feeds;
    return this;
  }
  private Double BW_framework;
  public Double get_BW_framework() {
    return BW_framework;
  }
  public void set_BW_framework(Double BW_framework) {
    this.BW_framework = BW_framework;
  }
  public Nutanix with_BW_framework(Double BW_framework) {
    this.BW_framework = BW_framework;
    return this;
  }
  private Double BW_hosting;
  public Double get_BW_hosting() {
    return BW_hosting;
  }
  public void set_BW_hosting(Double BW_hosting) {
    this.BW_hosting = BW_hosting;
  }
  public Nutanix with_BW_hosting(Double BW_hosting) {
    this.BW_hosting = BW_hosting;
    return this;
  }
  private Double BW_javascript;
  public Double get_BW_javascript() {
    return BW_javascript;
  }
  public void set_BW_javascript(Double BW_javascript) {
    this.BW_javascript = BW_javascript;
  }
  public Nutanix with_BW_javascript(Double BW_javascript) {
    this.BW_javascript = BW_javascript;
    return this;
  }
  private Double BW_mapping;
  public Double get_BW_mapping() {
    return BW_mapping;
  }
  public void set_BW_mapping(Double BW_mapping) {
    this.BW_mapping = BW_mapping;
  }
  public Nutanix with_BW_mapping(Double BW_mapping) {
    this.BW_mapping = BW_mapping;
    return this;
  }
  private Double BW_media;
  public Double get_BW_media() {
    return BW_media;
  }
  public void set_BW_media(Double BW_media) {
    this.BW_media = BW_media;
  }
  public Nutanix with_BW_media(Double BW_media) {
    this.BW_media = BW_media;
    return this;
  }
  private Double BW_mx;
  public Double get_BW_mx() {
    return BW_mx;
  }
  public void set_BW_mx(Double BW_mx) {
    this.BW_mx = BW_mx;
  }
  public Nutanix with_BW_mx(Double BW_mx) {
    this.BW_mx = BW_mx;
    return this;
  }
  private Double BW_ns;
  public Double get_BW_ns() {
    return BW_ns;
  }
  public void set_BW_ns(Double BW_ns) {
    this.BW_ns = BW_ns;
  }
  public Nutanix with_BW_ns(Double BW_ns) {
    this.BW_ns = BW_ns;
    return this;
  }
  private Double BW_parked;
  public Double get_BW_parked() {
    return BW_parked;
  }
  public void set_BW_parked(Double BW_parked) {
    this.BW_parked = BW_parked;
  }
  public Nutanix with_BW_parked(Double BW_parked) {
    this.BW_parked = BW_parked;
    return this;
  }
  private Double BW_payment;
  public Double get_BW_payment() {
    return BW_payment;
  }
  public void set_BW_payment(Double BW_payment) {
    this.BW_payment = BW_payment;
  }
  public Nutanix with_BW_payment(Double BW_payment) {
    this.BW_payment = BW_payment;
    return this;
  }
  private Double BW_seo_headers;
  public Double get_BW_seo_headers() {
    return BW_seo_headers;
  }
  public void set_BW_seo_headers(Double BW_seo_headers) {
    this.BW_seo_headers = BW_seo_headers;
  }
  public Nutanix with_BW_seo_headers(Double BW_seo_headers) {
    this.BW_seo_headers = BW_seo_headers;
    return this;
  }
  private Double BW_seo_meta;
  public Double get_BW_seo_meta() {
    return BW_seo_meta;
  }
  public void set_BW_seo_meta(Double BW_seo_meta) {
    this.BW_seo_meta = BW_seo_meta;
  }
  public Nutanix with_BW_seo_meta(Double BW_seo_meta) {
    this.BW_seo_meta = BW_seo_meta;
    return this;
  }
  private Double BW_seo_title;
  public Double get_BW_seo_title() {
    return BW_seo_title;
  }
  public void set_BW_seo_title(Double BW_seo_title) {
    this.BW_seo_title = BW_seo_title;
  }
  public Nutanix with_BW_seo_title(Double BW_seo_title) {
    this.BW_seo_title = BW_seo_title;
    return this;
  }
  private Double BW_Server;
  public Double get_BW_Server() {
    return BW_Server;
  }
  public void set_BW_Server(Double BW_Server) {
    this.BW_Server = BW_Server;
  }
  public Nutanix with_BW_Server(Double BW_Server) {
    this.BW_Server = BW_Server;
    return this;
  }
  private Double BW_shop;
  public Double get_BW_shop() {
    return BW_shop;
  }
  public void set_BW_shop(Double BW_shop) {
    this.BW_shop = BW_shop;
  }
  public Nutanix with_BW_shop(Double BW_shop) {
    this.BW_shop = BW_shop;
    return this;
  }
  private Double BW_ssl;
  public Double get_BW_ssl() {
    return BW_ssl;
  }
  public void set_BW_ssl(Double BW_ssl) {
    this.BW_ssl = BW_ssl;
  }
  public Nutanix with_BW_ssl(Double BW_ssl) {
    this.BW_ssl = BW_ssl;
    return this;
  }
  private Double BW_Web_Master;
  public Double get_BW_Web_Master() {
    return BW_Web_Master;
  }
  public void set_BW_Web_Master(Double BW_Web_Master) {
    this.BW_Web_Master = BW_Web_Master;
  }
  public Nutanix with_BW_Web_Master(Double BW_Web_Master) {
    this.BW_Web_Master = BW_Web_Master;
    return this;
  }
  private Double BW_Web_Server;
  public Double get_BW_Web_Server() {
    return BW_Web_Server;
  }
  public void set_BW_Web_Server(Double BW_Web_Server) {
    this.BW_Web_Server = BW_Web_Server;
  }
  public Nutanix with_BW_Web_Server(Double BW_Web_Server) {
    this.BW_Web_Server = BW_Web_Server;
    return this;
  }
  private Double BW_widgets;
  public Double get_BW_widgets() {
    return BW_widgets;
  }
  public void set_BW_widgets(Double BW_widgets) {
    this.BW_widgets = BW_widgets;
  }
  public Nutanix with_BW_widgets(Double BW_widgets) {
    this.BW_widgets = BW_widgets;
    return this;
  }
  private Double Activity_ClickLink_cnt;
  public Double get_Activity_ClickLink_cnt() {
    return Activity_ClickLink_cnt;
  }
  public void set_Activity_ClickLink_cnt(Double Activity_ClickLink_cnt) {
    this.Activity_ClickLink_cnt = Activity_ClickLink_cnt;
  }
  public Nutanix with_Activity_ClickLink_cnt(Double Activity_ClickLink_cnt) {
    this.Activity_ClickLink_cnt = Activity_ClickLink_cnt;
    return this;
  }
  private Double Activity_VisitWeb_cnt;
  public Double get_Activity_VisitWeb_cnt() {
    return Activity_VisitWeb_cnt;
  }
  public void set_Activity_VisitWeb_cnt(Double Activity_VisitWeb_cnt) {
    this.Activity_VisitWeb_cnt = Activity_VisitWeb_cnt;
  }
  public Nutanix with_Activity_VisitWeb_cnt(Double Activity_VisitWeb_cnt) {
    this.Activity_VisitWeb_cnt = Activity_VisitWeb_cnt;
    return this;
  }
  private Double Activity_InterestingMoment_cnt;
  public Double get_Activity_InterestingMoment_cnt() {
    return Activity_InterestingMoment_cnt;
  }
  public void set_Activity_InterestingMoment_cnt(Double Activity_InterestingMoment_cnt) {
    this.Activity_InterestingMoment_cnt = Activity_InterestingMoment_cnt;
  }
  public Nutanix with_Activity_InterestingMoment_cnt(Double Activity_InterestingMoment_cnt) {
    this.Activity_InterestingMoment_cnt = Activity_InterestingMoment_cnt;
    return this;
  }
  private Double Activity_OpenEmail_cnt;
  public Double get_Activity_OpenEmail_cnt() {
    return Activity_OpenEmail_cnt;
  }
  public void set_Activity_OpenEmail_cnt(Double Activity_OpenEmail_cnt) {
    this.Activity_OpenEmail_cnt = Activity_OpenEmail_cnt;
  }
  public Nutanix with_Activity_OpenEmail_cnt(Double Activity_OpenEmail_cnt) {
    this.Activity_OpenEmail_cnt = Activity_OpenEmail_cnt;
    return this;
  }
  private Double Activity_EmailBncedSft_cnt;
  public Double get_Activity_EmailBncedSft_cnt() {
    return Activity_EmailBncedSft_cnt;
  }
  public void set_Activity_EmailBncedSft_cnt(Double Activity_EmailBncedSft_cnt) {
    this.Activity_EmailBncedSft_cnt = Activity_EmailBncedSft_cnt;
  }
  public Nutanix with_Activity_EmailBncedSft_cnt(Double Activity_EmailBncedSft_cnt) {
    this.Activity_EmailBncedSft_cnt = Activity_EmailBncedSft_cnt;
    return this;
  }
  private Double Activity_FillOutForm_cnt;
  public Double get_Activity_FillOutForm_cnt() {
    return Activity_FillOutForm_cnt;
  }
  public void set_Activity_FillOutForm_cnt(Double Activity_FillOutForm_cnt) {
    this.Activity_FillOutForm_cnt = Activity_FillOutForm_cnt;
  }
  public Nutanix with_Activity_FillOutForm_cnt(Double Activity_FillOutForm_cnt) {
    this.Activity_FillOutForm_cnt = Activity_FillOutForm_cnt;
    return this;
  }
  private Double Activity_UnsubscrbEmail_cnt;
  public Double get_Activity_UnsubscrbEmail_cnt() {
    return Activity_UnsubscrbEmail_cnt;
  }
  public void set_Activity_UnsubscrbEmail_cnt(Double Activity_UnsubscrbEmail_cnt) {
    this.Activity_UnsubscrbEmail_cnt = Activity_UnsubscrbEmail_cnt;
  }
  public Nutanix with_Activity_UnsubscrbEmail_cnt(Double Activity_UnsubscrbEmail_cnt) {
    this.Activity_UnsubscrbEmail_cnt = Activity_UnsubscrbEmail_cnt;
    return this;
  }
  private Double Activity_ClickEmail_cnt;
  public Double get_Activity_ClickEmail_cnt() {
    return Activity_ClickEmail_cnt;
  }
  public void set_Activity_ClickEmail_cnt(Double Activity_ClickEmail_cnt) {
    this.Activity_ClickEmail_cnt = Activity_ClickEmail_cnt;
  }
  public Nutanix with_Activity_ClickEmail_cnt(Double Activity_ClickEmail_cnt) {
    this.Activity_ClickEmail_cnt = Activity_ClickEmail_cnt;
    return this;
  }
  private Double CloudTechnologies_CloudService_One;
  public Double get_CloudTechnologies_CloudService_One() {
    return CloudTechnologies_CloudService_One;
  }
  public void set_CloudTechnologies_CloudService_One(Double CloudTechnologies_CloudService_One) {
    this.CloudTechnologies_CloudService_One = CloudTechnologies_CloudService_One;
  }
  public Nutanix with_CloudTechnologies_CloudService_One(Double CloudTechnologies_CloudService_One) {
    this.CloudTechnologies_CloudService_One = CloudTechnologies_CloudService_One;
    return this;
  }
  private Double CloudTechnologies_CloudService_Two;
  public Double get_CloudTechnologies_CloudService_Two() {
    return CloudTechnologies_CloudService_Two;
  }
  public void set_CloudTechnologies_CloudService_Two(Double CloudTechnologies_CloudService_Two) {
    this.CloudTechnologies_CloudService_Two = CloudTechnologies_CloudService_Two;
  }
  public Nutanix with_CloudTechnologies_CloudService_Two(Double CloudTechnologies_CloudService_Two) {
    this.CloudTechnologies_CloudService_Two = CloudTechnologies_CloudService_Two;
    return this;
  }
  private Double CloudTechnologies_CommTech_One;
  public Double get_CloudTechnologies_CommTech_One() {
    return CloudTechnologies_CommTech_One;
  }
  public void set_CloudTechnologies_CommTech_One(Double CloudTechnologies_CommTech_One) {
    this.CloudTechnologies_CommTech_One = CloudTechnologies_CommTech_One;
  }
  public Nutanix with_CloudTechnologies_CommTech_One(Double CloudTechnologies_CommTech_One) {
    this.CloudTechnologies_CommTech_One = CloudTechnologies_CommTech_One;
    return this;
  }
  private Double CloudTechnologies_CommTech_Two;
  public Double get_CloudTechnologies_CommTech_Two() {
    return CloudTechnologies_CommTech_Two;
  }
  public void set_CloudTechnologies_CommTech_Two(Double CloudTechnologies_CommTech_Two) {
    this.CloudTechnologies_CommTech_Two = CloudTechnologies_CommTech_Two;
  }
  public Nutanix with_CloudTechnologies_CommTech_Two(Double CloudTechnologies_CommTech_Two) {
    this.CloudTechnologies_CommTech_Two = CloudTechnologies_CommTech_Two;
    return this;
  }
  private Double CloudTechnologies_CRM_One;
  public Double get_CloudTechnologies_CRM_One() {
    return CloudTechnologies_CRM_One;
  }
  public void set_CloudTechnologies_CRM_One(Double CloudTechnologies_CRM_One) {
    this.CloudTechnologies_CRM_One = CloudTechnologies_CRM_One;
  }
  public Nutanix with_CloudTechnologies_CRM_One(Double CloudTechnologies_CRM_One) {
    this.CloudTechnologies_CRM_One = CloudTechnologies_CRM_One;
    return this;
  }
  private Double CloudTechnologies_CRM_Two;
  public Double get_CloudTechnologies_CRM_Two() {
    return CloudTechnologies_CRM_Two;
  }
  public void set_CloudTechnologies_CRM_Two(Double CloudTechnologies_CRM_Two) {
    this.CloudTechnologies_CRM_Two = CloudTechnologies_CRM_Two;
  }
  public Nutanix with_CloudTechnologies_CRM_Two(Double CloudTechnologies_CRM_Two) {
    this.CloudTechnologies_CRM_Two = CloudTechnologies_CRM_Two;
    return this;
  }
  private Double CloudTechnologies_DataCenterSolutions_One;
  public Double get_CloudTechnologies_DataCenterSolutions_One() {
    return CloudTechnologies_DataCenterSolutions_One;
  }
  public void set_CloudTechnologies_DataCenterSolutions_One(Double CloudTechnologies_DataCenterSolutions_One) {
    this.CloudTechnologies_DataCenterSolutions_One = CloudTechnologies_DataCenterSolutions_One;
  }
  public Nutanix with_CloudTechnologies_DataCenterSolutions_One(Double CloudTechnologies_DataCenterSolutions_One) {
    this.CloudTechnologies_DataCenterSolutions_One = CloudTechnologies_DataCenterSolutions_One;
    return this;
  }
  private Double CloudTechnologies_DataCenterSolutions_Two;
  public Double get_CloudTechnologies_DataCenterSolutions_Two() {
    return CloudTechnologies_DataCenterSolutions_Two;
  }
  public void set_CloudTechnologies_DataCenterSolutions_Two(Double CloudTechnologies_DataCenterSolutions_Two) {
    this.CloudTechnologies_DataCenterSolutions_Two = CloudTechnologies_DataCenterSolutions_Two;
  }
  public Nutanix with_CloudTechnologies_DataCenterSolutions_Two(Double CloudTechnologies_DataCenterSolutions_Two) {
    this.CloudTechnologies_DataCenterSolutions_Two = CloudTechnologies_DataCenterSolutions_Two;
    return this;
  }
  private Double CloudTechnologies_EnterpriseApplications_One;
  public Double get_CloudTechnologies_EnterpriseApplications_One() {
    return CloudTechnologies_EnterpriseApplications_One;
  }
  public void set_CloudTechnologies_EnterpriseApplications_One(Double CloudTechnologies_EnterpriseApplications_One) {
    this.CloudTechnologies_EnterpriseApplications_One = CloudTechnologies_EnterpriseApplications_One;
  }
  public Nutanix with_CloudTechnologies_EnterpriseApplications_One(Double CloudTechnologies_EnterpriseApplications_One) {
    this.CloudTechnologies_EnterpriseApplications_One = CloudTechnologies_EnterpriseApplications_One;
    return this;
  }
  private Double CloudTechnologies_EnterpriseApplications_Two;
  public Double get_CloudTechnologies_EnterpriseApplications_Two() {
    return CloudTechnologies_EnterpriseApplications_Two;
  }
  public void set_CloudTechnologies_EnterpriseApplications_Two(Double CloudTechnologies_EnterpriseApplications_Two) {
    this.CloudTechnologies_EnterpriseApplications_Two = CloudTechnologies_EnterpriseApplications_Two;
  }
  public Nutanix with_CloudTechnologies_EnterpriseApplications_Two(Double CloudTechnologies_EnterpriseApplications_Two) {
    this.CloudTechnologies_EnterpriseApplications_Two = CloudTechnologies_EnterpriseApplications_Two;
    return this;
  }
  private Double CloudTechnologies_EnterpriseContent_One;
  public Double get_CloudTechnologies_EnterpriseContent_One() {
    return CloudTechnologies_EnterpriseContent_One;
  }
  public void set_CloudTechnologies_EnterpriseContent_One(Double CloudTechnologies_EnterpriseContent_One) {
    this.CloudTechnologies_EnterpriseContent_One = CloudTechnologies_EnterpriseContent_One;
  }
  public Nutanix with_CloudTechnologies_EnterpriseContent_One(Double CloudTechnologies_EnterpriseContent_One) {
    this.CloudTechnologies_EnterpriseContent_One = CloudTechnologies_EnterpriseContent_One;
    return this;
  }
  private Double CloudTechnologies_EnterpriseContent_Two;
  public Double get_CloudTechnologies_EnterpriseContent_Two() {
    return CloudTechnologies_EnterpriseContent_Two;
  }
  public void set_CloudTechnologies_EnterpriseContent_Two(Double CloudTechnologies_EnterpriseContent_Two) {
    this.CloudTechnologies_EnterpriseContent_Two = CloudTechnologies_EnterpriseContent_Two;
  }
  public Nutanix with_CloudTechnologies_EnterpriseContent_Two(Double CloudTechnologies_EnterpriseContent_Two) {
    this.CloudTechnologies_EnterpriseContent_Two = CloudTechnologies_EnterpriseContent_Two;
    return this;
  }
  private Double CloudTechnologies_HardwareBasic_One;
  public Double get_CloudTechnologies_HardwareBasic_One() {
    return CloudTechnologies_HardwareBasic_One;
  }
  public void set_CloudTechnologies_HardwareBasic_One(Double CloudTechnologies_HardwareBasic_One) {
    this.CloudTechnologies_HardwareBasic_One = CloudTechnologies_HardwareBasic_One;
  }
  public Nutanix with_CloudTechnologies_HardwareBasic_One(Double CloudTechnologies_HardwareBasic_One) {
    this.CloudTechnologies_HardwareBasic_One = CloudTechnologies_HardwareBasic_One;
    return this;
  }
  private Double CloudTechnologies_HardwareBasic_Two;
  public Double get_CloudTechnologies_HardwareBasic_Two() {
    return CloudTechnologies_HardwareBasic_Two;
  }
  public void set_CloudTechnologies_HardwareBasic_Two(Double CloudTechnologies_HardwareBasic_Two) {
    this.CloudTechnologies_HardwareBasic_Two = CloudTechnologies_HardwareBasic_Two;
  }
  public Nutanix with_CloudTechnologies_HardwareBasic_Two(Double CloudTechnologies_HardwareBasic_Two) {
    this.CloudTechnologies_HardwareBasic_Two = CloudTechnologies_HardwareBasic_Two;
    return this;
  }
  private Double CloudTechnologies_ITGovernance_One;
  public Double get_CloudTechnologies_ITGovernance_One() {
    return CloudTechnologies_ITGovernance_One;
  }
  public void set_CloudTechnologies_ITGovernance_One(Double CloudTechnologies_ITGovernance_One) {
    this.CloudTechnologies_ITGovernance_One = CloudTechnologies_ITGovernance_One;
  }
  public Nutanix with_CloudTechnologies_ITGovernance_One(Double CloudTechnologies_ITGovernance_One) {
    this.CloudTechnologies_ITGovernance_One = CloudTechnologies_ITGovernance_One;
    return this;
  }
  private Double CloudTechnologies_ITGovernance_Two;
  public Double get_CloudTechnologies_ITGovernance_Two() {
    return CloudTechnologies_ITGovernance_Two;
  }
  public void set_CloudTechnologies_ITGovernance_Two(Double CloudTechnologies_ITGovernance_Two) {
    this.CloudTechnologies_ITGovernance_Two = CloudTechnologies_ITGovernance_Two;
  }
  public Nutanix with_CloudTechnologies_ITGovernance_Two(Double CloudTechnologies_ITGovernance_Two) {
    this.CloudTechnologies_ITGovernance_Two = CloudTechnologies_ITGovernance_Two;
    return this;
  }
  private Double CloudTechnologies_MarketingPerfMgmt_One;
  public Double get_CloudTechnologies_MarketingPerfMgmt_One() {
    return CloudTechnologies_MarketingPerfMgmt_One;
  }
  public void set_CloudTechnologies_MarketingPerfMgmt_One(Double CloudTechnologies_MarketingPerfMgmt_One) {
    this.CloudTechnologies_MarketingPerfMgmt_One = CloudTechnologies_MarketingPerfMgmt_One;
  }
  public Nutanix with_CloudTechnologies_MarketingPerfMgmt_One(Double CloudTechnologies_MarketingPerfMgmt_One) {
    this.CloudTechnologies_MarketingPerfMgmt_One = CloudTechnologies_MarketingPerfMgmt_One;
    return this;
  }
  private Double CloudTechnologies_MarketingPerfMgmt_Two;
  public Double get_CloudTechnologies_MarketingPerfMgmt_Two() {
    return CloudTechnologies_MarketingPerfMgmt_Two;
  }
  public void set_CloudTechnologies_MarketingPerfMgmt_Two(Double CloudTechnologies_MarketingPerfMgmt_Two) {
    this.CloudTechnologies_MarketingPerfMgmt_Two = CloudTechnologies_MarketingPerfMgmt_Two;
  }
  public Nutanix with_CloudTechnologies_MarketingPerfMgmt_Two(Double CloudTechnologies_MarketingPerfMgmt_Two) {
    this.CloudTechnologies_MarketingPerfMgmt_Two = CloudTechnologies_MarketingPerfMgmt_Two;
    return this;
  }
  private Double CloudTechnologies_NetworkComputing_One;
  public Double get_CloudTechnologies_NetworkComputing_One() {
    return CloudTechnologies_NetworkComputing_One;
  }
  public void set_CloudTechnologies_NetworkComputing_One(Double CloudTechnologies_NetworkComputing_One) {
    this.CloudTechnologies_NetworkComputing_One = CloudTechnologies_NetworkComputing_One;
  }
  public Nutanix with_CloudTechnologies_NetworkComputing_One(Double CloudTechnologies_NetworkComputing_One) {
    this.CloudTechnologies_NetworkComputing_One = CloudTechnologies_NetworkComputing_One;
    return this;
  }
  private Double CloudTechnologies_NetworkComputing_Two;
  public Double get_CloudTechnologies_NetworkComputing_Two() {
    return CloudTechnologies_NetworkComputing_Two;
  }
  public void set_CloudTechnologies_NetworkComputing_Two(Double CloudTechnologies_NetworkComputing_Two) {
    this.CloudTechnologies_NetworkComputing_Two = CloudTechnologies_NetworkComputing_Two;
  }
  public Nutanix with_CloudTechnologies_NetworkComputing_Two(Double CloudTechnologies_NetworkComputing_Two) {
    this.CloudTechnologies_NetworkComputing_Two = CloudTechnologies_NetworkComputing_Two;
    return this;
  }
  private Double CloudTechnologies_ProductivitySltns_One;
  public Double get_CloudTechnologies_ProductivitySltns_One() {
    return CloudTechnologies_ProductivitySltns_One;
  }
  public void set_CloudTechnologies_ProductivitySltns_One(Double CloudTechnologies_ProductivitySltns_One) {
    this.CloudTechnologies_ProductivitySltns_One = CloudTechnologies_ProductivitySltns_One;
  }
  public Nutanix with_CloudTechnologies_ProductivitySltns_One(Double CloudTechnologies_ProductivitySltns_One) {
    this.CloudTechnologies_ProductivitySltns_One = CloudTechnologies_ProductivitySltns_One;
    return this;
  }
  private Double CloudTechnologies_ProductivitySltns_Two;
  public Double get_CloudTechnologies_ProductivitySltns_Two() {
    return CloudTechnologies_ProductivitySltns_Two;
  }
  public void set_CloudTechnologies_ProductivitySltns_Two(Double CloudTechnologies_ProductivitySltns_Two) {
    this.CloudTechnologies_ProductivitySltns_Two = CloudTechnologies_ProductivitySltns_Two;
  }
  public Nutanix with_CloudTechnologies_ProductivitySltns_Two(Double CloudTechnologies_ProductivitySltns_Two) {
    this.CloudTechnologies_ProductivitySltns_Two = CloudTechnologies_ProductivitySltns_Two;
    return this;
  }
  private Double CloudTechnologies_ProjectMgnt_One;
  public Double get_CloudTechnologies_ProjectMgnt_One() {
    return CloudTechnologies_ProjectMgnt_One;
  }
  public void set_CloudTechnologies_ProjectMgnt_One(Double CloudTechnologies_ProjectMgnt_One) {
    this.CloudTechnologies_ProjectMgnt_One = CloudTechnologies_ProjectMgnt_One;
  }
  public Nutanix with_CloudTechnologies_ProjectMgnt_One(Double CloudTechnologies_ProjectMgnt_One) {
    this.CloudTechnologies_ProjectMgnt_One = CloudTechnologies_ProjectMgnt_One;
    return this;
  }
  private Double CloudTechnologies_ProjectMgnt_Two;
  public Double get_CloudTechnologies_ProjectMgnt_Two() {
    return CloudTechnologies_ProjectMgnt_Two;
  }
  public void set_CloudTechnologies_ProjectMgnt_Two(Double CloudTechnologies_ProjectMgnt_Two) {
    this.CloudTechnologies_ProjectMgnt_Two = CloudTechnologies_ProjectMgnt_Two;
  }
  public Nutanix with_CloudTechnologies_ProjectMgnt_Two(Double CloudTechnologies_ProjectMgnt_Two) {
    this.CloudTechnologies_ProjectMgnt_Two = CloudTechnologies_ProjectMgnt_Two;
    return this;
  }
  private Double CloudTechnologies_SoftwareBasic_One;
  public Double get_CloudTechnologies_SoftwareBasic_One() {
    return CloudTechnologies_SoftwareBasic_One;
  }
  public void set_CloudTechnologies_SoftwareBasic_One(Double CloudTechnologies_SoftwareBasic_One) {
    this.CloudTechnologies_SoftwareBasic_One = CloudTechnologies_SoftwareBasic_One;
  }
  public Nutanix with_CloudTechnologies_SoftwareBasic_One(Double CloudTechnologies_SoftwareBasic_One) {
    this.CloudTechnologies_SoftwareBasic_One = CloudTechnologies_SoftwareBasic_One;
    return this;
  }
  private Double CloudTechnologies_SoftwareBasic_Two;
  public Double get_CloudTechnologies_SoftwareBasic_Two() {
    return CloudTechnologies_SoftwareBasic_Two;
  }
  public void set_CloudTechnologies_SoftwareBasic_Two(Double CloudTechnologies_SoftwareBasic_Two) {
    this.CloudTechnologies_SoftwareBasic_Two = CloudTechnologies_SoftwareBasic_Two;
  }
  public Nutanix with_CloudTechnologies_SoftwareBasic_Two(Double CloudTechnologies_SoftwareBasic_Two) {
    this.CloudTechnologies_SoftwareBasic_Two = CloudTechnologies_SoftwareBasic_Two;
    return this;
  }
  private Double CloudTechnologies_VerticalMarkets_One;
  public Double get_CloudTechnologies_VerticalMarkets_One() {
    return CloudTechnologies_VerticalMarkets_One;
  }
  public void set_CloudTechnologies_VerticalMarkets_One(Double CloudTechnologies_VerticalMarkets_One) {
    this.CloudTechnologies_VerticalMarkets_One = CloudTechnologies_VerticalMarkets_One;
  }
  public Nutanix with_CloudTechnologies_VerticalMarkets_One(Double CloudTechnologies_VerticalMarkets_One) {
    this.CloudTechnologies_VerticalMarkets_One = CloudTechnologies_VerticalMarkets_One;
    return this;
  }
  private Double CloudTechnologies_VerticalMarkets_Two;
  public Double get_CloudTechnologies_VerticalMarkets_Two() {
    return CloudTechnologies_VerticalMarkets_Two;
  }
  public void set_CloudTechnologies_VerticalMarkets_Two(Double CloudTechnologies_VerticalMarkets_Two) {
    this.CloudTechnologies_VerticalMarkets_Two = CloudTechnologies_VerticalMarkets_Two;
  }
  public Nutanix with_CloudTechnologies_VerticalMarkets_Two(Double CloudTechnologies_VerticalMarkets_Two) {
    this.CloudTechnologies_VerticalMarkets_Two = CloudTechnologies_VerticalMarkets_Two;
    return this;
  }
  private Double CloudTechnologies_WebOrntdArch_One;
  public Double get_CloudTechnologies_WebOrntdArch_One() {
    return CloudTechnologies_WebOrntdArch_One;
  }
  public void set_CloudTechnologies_WebOrntdArch_One(Double CloudTechnologies_WebOrntdArch_One) {
    this.CloudTechnologies_WebOrntdArch_One = CloudTechnologies_WebOrntdArch_One;
  }
  public Nutanix with_CloudTechnologies_WebOrntdArch_One(Double CloudTechnologies_WebOrntdArch_One) {
    this.CloudTechnologies_WebOrntdArch_One = CloudTechnologies_WebOrntdArch_One;
    return this;
  }
  private Double CloudTechnologies_WebOrntdArch_Two;
  public Double get_CloudTechnologies_WebOrntdArch_Two() {
    return CloudTechnologies_WebOrntdArch_Two;
  }
  public void set_CloudTechnologies_WebOrntdArch_Two(Double CloudTechnologies_WebOrntdArch_Two) {
    this.CloudTechnologies_WebOrntdArch_Two = CloudTechnologies_WebOrntdArch_Two;
  }
  public Nutanix with_CloudTechnologies_WebOrntdArch_Two(Double CloudTechnologies_WebOrntdArch_Two) {
    this.CloudTechnologies_WebOrntdArch_Two = CloudTechnologies_WebOrntdArch_Two;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Nutanix)) {
      return false;
    }
    Nutanix that = (Nutanix) o;
    boolean equal = true;
    equal = equal && (this.Nutanix_EventTable_Clean == null ? that.Nutanix_EventTable_Clean == null : this.Nutanix_EventTable_Clean.equals(that.Nutanix_EventTable_Clean));
    equal = equal && (this.LeadID == null ? that.LeadID == null : this.LeadID.equals(that.LeadID));
    equal = equal && (this.CustomerID == null ? that.CustomerID == null : this.CustomerID.equals(that.CustomerID));
    equal = equal && (this.PeriodID == null ? that.PeriodID == null : this.PeriodID.equals(that.PeriodID));
    equal = equal && (this.P1_Target == null ? that.P1_Target == null : this.P1_Target.equals(that.P1_Target));
    equal = equal && (this.P1_TargetTraining == null ? that.P1_TargetTraining == null : this.P1_TargetTraining.equals(that.P1_TargetTraining));
    equal = equal && (this.P1_Event == null ? that.P1_Event == null : this.P1_Event.equals(that.P1_Event));
    equal = equal && (this.Company == null ? that.Company == null : this.Company.equals(that.Company));
    equal = equal && (this.Email == null ? that.Email == null : this.Email.equals(that.Email));
    equal = equal && (this.Domain == null ? that.Domain == null : this.Domain.equals(that.Domain));
    equal = equal && (this.AwardYear == null ? that.AwardYear == null : this.AwardYear.equals(that.AwardYear));
    equal = equal && (this.BankruptcyFiled == null ? that.BankruptcyFiled == null : this.BankruptcyFiled.equals(that.BankruptcyFiled));
    equal = equal && (this.BusinessAnnualSalesAbs == null ? that.BusinessAnnualSalesAbs == null : this.BusinessAnnualSalesAbs.equals(that.BusinessAnnualSalesAbs));
    equal = equal && (this.BusinessAssets == null ? that.BusinessAssets == null : this.BusinessAssets.equals(that.BusinessAssets));
    equal = equal && (this.BusinessECommerceSite == null ? that.BusinessECommerceSite == null : this.BusinessECommerceSite.equals(that.BusinessECommerceSite));
    equal = equal && (this.BusinessEntityType == null ? that.BusinessEntityType == null : this.BusinessEntityType.equals(that.BusinessEntityType));
    equal = equal && (this.BusinessEstablishedYear == null ? that.BusinessEstablishedYear == null : this.BusinessEstablishedYear.equals(that.BusinessEstablishedYear));
    equal = equal && (this.BusinessEstimatedAnnualSales_k == null ? that.BusinessEstimatedAnnualSales_k == null : this.BusinessEstimatedAnnualSales_k.equals(that.BusinessEstimatedAnnualSales_k));
    equal = equal && (this.BusinessEstimatedEmployees == null ? that.BusinessEstimatedEmployees == null : this.BusinessEstimatedEmployees.equals(that.BusinessEstimatedEmployees));
    equal = equal && (this.BusinessFirmographicsParentEmployees == null ? that.BusinessFirmographicsParentEmployees == null : this.BusinessFirmographicsParentEmployees.equals(that.BusinessFirmographicsParentEmployees));
    equal = equal && (this.BusinessFirmographicsParentRevenue == null ? that.BusinessFirmographicsParentRevenue == null : this.BusinessFirmographicsParentRevenue.equals(that.BusinessFirmographicsParentRevenue));
    equal = equal && (this.BusinessIndustrySector == null ? that.BusinessIndustrySector == null : this.BusinessIndustrySector.equals(that.BusinessIndustrySector));
    equal = equal && (this.BusinessRetirementParticipants == null ? that.BusinessRetirementParticipants == null : this.BusinessRetirementParticipants.equals(that.BusinessRetirementParticipants));
    equal = equal && (this.BusinessSocialPresence == null ? that.BusinessSocialPresence == null : this.BusinessSocialPresence.equals(that.BusinessSocialPresence));
    equal = equal && (this.BusinessUrlNumPages == null ? that.BusinessUrlNumPages == null : this.BusinessUrlNumPages.equals(that.BusinessUrlNumPages));
    equal = equal && (this.BusinessType == null ? that.BusinessType == null : this.BusinessType.equals(that.BusinessType));
    equal = equal && (this.BusinessVCFunded == null ? that.BusinessVCFunded == null : this.BusinessVCFunded.equals(that.BusinessVCFunded));
    equal = equal && (this.DerogatoryIndicator == null ? that.DerogatoryIndicator == null : this.DerogatoryIndicator.equals(that.DerogatoryIndicator));
    equal = equal && (this.ExperianCreditRating == null ? that.ExperianCreditRating == null : this.ExperianCreditRating.equals(that.ExperianCreditRating));
    equal = equal && (this.FundingAgency == null ? that.FundingAgency == null : this.FundingAgency.equals(that.FundingAgency));
    equal = equal && (this.FundingAmount == null ? that.FundingAmount == null : this.FundingAmount.equals(that.FundingAmount));
    equal = equal && (this.FundingAwardAmount == null ? that.FundingAwardAmount == null : this.FundingAwardAmount.equals(that.FundingAwardAmount));
    equal = equal && (this.FundingFinanceRound == null ? that.FundingFinanceRound == null : this.FundingFinanceRound.equals(that.FundingFinanceRound));
    equal = equal && (this.FundingFiscalQuarter == null ? that.FundingFiscalQuarter == null : this.FundingFiscalQuarter.equals(that.FundingFiscalQuarter));
    equal = equal && (this.FundingFiscalYear == null ? that.FundingFiscalYear == null : this.FundingFiscalYear.equals(that.FundingFiscalYear));
    equal = equal && (this.FundingReceived == null ? that.FundingReceived == null : this.FundingReceived.equals(that.FundingReceived));
    equal = equal && (this.FundingStage == null ? that.FundingStage == null : this.FundingStage.equals(that.FundingStage));
    equal = equal && (this.Intelliscore == null ? that.Intelliscore == null : this.Intelliscore.equals(that.Intelliscore));
    equal = equal && (this.JobsRecentJobs == null ? that.JobsRecentJobs == null : this.JobsRecentJobs.equals(that.JobsRecentJobs));
    equal = equal && (this.JobsTrendString == null ? that.JobsTrendString == null : this.JobsTrendString.equals(that.JobsTrendString));
    equal = equal && (this.ModelAction == null ? that.ModelAction == null : this.ModelAction.equals(that.ModelAction));
    equal = equal && (this.PercentileModel == null ? that.PercentileModel == null : this.PercentileModel.equals(that.PercentileModel));
    equal = equal && (this.RetirementAssetsEOY == null ? that.RetirementAssetsEOY == null : this.RetirementAssetsEOY.equals(that.RetirementAssetsEOY));
    equal = equal && (this.RetirementAssetsYOY == null ? that.RetirementAssetsYOY == null : this.RetirementAssetsYOY.equals(that.RetirementAssetsYOY));
    equal = equal && (this.TotalParticipantsSOY == null ? that.TotalParticipantsSOY == null : this.TotalParticipantsSOY.equals(that.TotalParticipantsSOY));
    equal = equal && (this.UCCFilings == null ? that.UCCFilings == null : this.UCCFilings.equals(that.UCCFilings));
    equal = equal && (this.UCCFilingsPresent == null ? that.UCCFilingsPresent == null : this.UCCFilingsPresent.equals(that.UCCFilingsPresent));
    equal = equal && (this.Years_in_Business_Code == null ? that.Years_in_Business_Code == null : this.Years_in_Business_Code.equals(that.Years_in_Business_Code));
    equal = equal && (this.Non_Profit_Indicator == null ? that.Non_Profit_Indicator == null : this.Non_Profit_Indicator.equals(that.Non_Profit_Indicator));
    equal = equal && (this.PD_DA_AwardCategory == null ? that.PD_DA_AwardCategory == null : this.PD_DA_AwardCategory.equals(that.PD_DA_AwardCategory));
    equal = equal && (this.PD_DA_JobTitle == null ? that.PD_DA_JobTitle == null : this.PD_DA_JobTitle.equals(that.PD_DA_JobTitle));
    equal = equal && (this.PD_DA_LastSocialActivity_Units == null ? that.PD_DA_LastSocialActivity_Units == null : this.PD_DA_LastSocialActivity_Units.equals(that.PD_DA_LastSocialActivity_Units));
    equal = equal && (this.PD_DA_MonthsPatentGranted == null ? that.PD_DA_MonthsPatentGranted == null : this.PD_DA_MonthsPatentGranted.equals(that.PD_DA_MonthsPatentGranted));
    equal = equal && (this.PD_DA_MonthsSinceFundAwardDate == null ? that.PD_DA_MonthsSinceFundAwardDate == null : this.PD_DA_MonthsSinceFundAwardDate.equals(that.PD_DA_MonthsSinceFundAwardDate));
    equal = equal && (this.PD_DA_PrimarySIC1 == null ? that.PD_DA_PrimarySIC1 == null : this.PD_DA_PrimarySIC1.equals(that.PD_DA_PrimarySIC1));
    equal = equal && (this.Industry == null ? that.Industry == null : this.Industry.equals(that.Industry));
    equal = equal && (this.LeadSource == null ? that.LeadSource == null : this.LeadSource.equals(that.LeadSource));
    equal = equal && (this.AnnualRevenue == null ? that.AnnualRevenue == null : this.AnnualRevenue.equals(that.AnnualRevenue));
    equal = equal && (this.NumberOfEmployees == null ? that.NumberOfEmployees == null : this.NumberOfEmployees.equals(that.NumberOfEmployees));
    equal = equal && (this.Alexa_MonthsSinceOnline == null ? that.Alexa_MonthsSinceOnline == null : this.Alexa_MonthsSinceOnline.equals(that.Alexa_MonthsSinceOnline));
    equal = equal && (this.Alexa_Rank == null ? that.Alexa_Rank == null : this.Alexa_Rank.equals(that.Alexa_Rank));
    equal = equal && (this.Alexa_ReachPerMillion == null ? that.Alexa_ReachPerMillion == null : this.Alexa_ReachPerMillion.equals(that.Alexa_ReachPerMillion));
    equal = equal && (this.Alexa_ViewsPerMillion == null ? that.Alexa_ViewsPerMillion == null : this.Alexa_ViewsPerMillion.equals(that.Alexa_ViewsPerMillion));
    equal = equal && (this.Alexa_ViewsPerUser == null ? that.Alexa_ViewsPerUser == null : this.Alexa_ViewsPerUser.equals(that.Alexa_ViewsPerUser));
    equal = equal && (this.BW_TechTags_Cnt == null ? that.BW_TechTags_Cnt == null : this.BW_TechTags_Cnt.equals(that.BW_TechTags_Cnt));
    equal = equal && (this.BW_TotalTech_Cnt == null ? that.BW_TotalTech_Cnt == null : this.BW_TotalTech_Cnt.equals(that.BW_TotalTech_Cnt));
    equal = equal && (this.BW_ads == null ? that.BW_ads == null : this.BW_ads.equals(that.BW_ads));
    equal = equal && (this.BW_analytics == null ? that.BW_analytics == null : this.BW_analytics.equals(that.BW_analytics));
    equal = equal && (this.BW_cdn == null ? that.BW_cdn == null : this.BW_cdn.equals(that.BW_cdn));
    equal = equal && (this.BW_cdns == null ? that.BW_cdns == null : this.BW_cdns.equals(that.BW_cdns));
    equal = equal && (this.BW_cms == null ? that.BW_cms == null : this.BW_cms.equals(that.BW_cms));
    equal = equal && (this.BW_docinfo == null ? that.BW_docinfo == null : this.BW_docinfo.equals(that.BW_docinfo));
    equal = equal && (this.BW_encoding == null ? that.BW_encoding == null : this.BW_encoding.equals(that.BW_encoding));
    equal = equal && (this.BW_feeds == null ? that.BW_feeds == null : this.BW_feeds.equals(that.BW_feeds));
    equal = equal && (this.BW_framework == null ? that.BW_framework == null : this.BW_framework.equals(that.BW_framework));
    equal = equal && (this.BW_hosting == null ? that.BW_hosting == null : this.BW_hosting.equals(that.BW_hosting));
    equal = equal && (this.BW_javascript == null ? that.BW_javascript == null : this.BW_javascript.equals(that.BW_javascript));
    equal = equal && (this.BW_mapping == null ? that.BW_mapping == null : this.BW_mapping.equals(that.BW_mapping));
    equal = equal && (this.BW_media == null ? that.BW_media == null : this.BW_media.equals(that.BW_media));
    equal = equal && (this.BW_mx == null ? that.BW_mx == null : this.BW_mx.equals(that.BW_mx));
    equal = equal && (this.BW_ns == null ? that.BW_ns == null : this.BW_ns.equals(that.BW_ns));
    equal = equal && (this.BW_parked == null ? that.BW_parked == null : this.BW_parked.equals(that.BW_parked));
    equal = equal && (this.BW_payment == null ? that.BW_payment == null : this.BW_payment.equals(that.BW_payment));
    equal = equal && (this.BW_seo_headers == null ? that.BW_seo_headers == null : this.BW_seo_headers.equals(that.BW_seo_headers));
    equal = equal && (this.BW_seo_meta == null ? that.BW_seo_meta == null : this.BW_seo_meta.equals(that.BW_seo_meta));
    equal = equal && (this.BW_seo_title == null ? that.BW_seo_title == null : this.BW_seo_title.equals(that.BW_seo_title));
    equal = equal && (this.BW_Server == null ? that.BW_Server == null : this.BW_Server.equals(that.BW_Server));
    equal = equal && (this.BW_shop == null ? that.BW_shop == null : this.BW_shop.equals(that.BW_shop));
    equal = equal && (this.BW_ssl == null ? that.BW_ssl == null : this.BW_ssl.equals(that.BW_ssl));
    equal = equal && (this.BW_Web_Master == null ? that.BW_Web_Master == null : this.BW_Web_Master.equals(that.BW_Web_Master));
    equal = equal && (this.BW_Web_Server == null ? that.BW_Web_Server == null : this.BW_Web_Server.equals(that.BW_Web_Server));
    equal = equal && (this.BW_widgets == null ? that.BW_widgets == null : this.BW_widgets.equals(that.BW_widgets));
    equal = equal && (this.Activity_ClickLink_cnt == null ? that.Activity_ClickLink_cnt == null : this.Activity_ClickLink_cnt.equals(that.Activity_ClickLink_cnt));
    equal = equal && (this.Activity_VisitWeb_cnt == null ? that.Activity_VisitWeb_cnt == null : this.Activity_VisitWeb_cnt.equals(that.Activity_VisitWeb_cnt));
    equal = equal && (this.Activity_InterestingMoment_cnt == null ? that.Activity_InterestingMoment_cnt == null : this.Activity_InterestingMoment_cnt.equals(that.Activity_InterestingMoment_cnt));
    equal = equal && (this.Activity_OpenEmail_cnt == null ? that.Activity_OpenEmail_cnt == null : this.Activity_OpenEmail_cnt.equals(that.Activity_OpenEmail_cnt));
    equal = equal && (this.Activity_EmailBncedSft_cnt == null ? that.Activity_EmailBncedSft_cnt == null : this.Activity_EmailBncedSft_cnt.equals(that.Activity_EmailBncedSft_cnt));
    equal = equal && (this.Activity_FillOutForm_cnt == null ? that.Activity_FillOutForm_cnt == null : this.Activity_FillOutForm_cnt.equals(that.Activity_FillOutForm_cnt));
    equal = equal && (this.Activity_UnsubscrbEmail_cnt == null ? that.Activity_UnsubscrbEmail_cnt == null : this.Activity_UnsubscrbEmail_cnt.equals(that.Activity_UnsubscrbEmail_cnt));
    equal = equal && (this.Activity_ClickEmail_cnt == null ? that.Activity_ClickEmail_cnt == null : this.Activity_ClickEmail_cnt.equals(that.Activity_ClickEmail_cnt));
    equal = equal && (this.CloudTechnologies_CloudService_One == null ? that.CloudTechnologies_CloudService_One == null : this.CloudTechnologies_CloudService_One.equals(that.CloudTechnologies_CloudService_One));
    equal = equal && (this.CloudTechnologies_CloudService_Two == null ? that.CloudTechnologies_CloudService_Two == null : this.CloudTechnologies_CloudService_Two.equals(that.CloudTechnologies_CloudService_Two));
    equal = equal && (this.CloudTechnologies_CommTech_One == null ? that.CloudTechnologies_CommTech_One == null : this.CloudTechnologies_CommTech_One.equals(that.CloudTechnologies_CommTech_One));
    equal = equal && (this.CloudTechnologies_CommTech_Two == null ? that.CloudTechnologies_CommTech_Two == null : this.CloudTechnologies_CommTech_Two.equals(that.CloudTechnologies_CommTech_Two));
    equal = equal && (this.CloudTechnologies_CRM_One == null ? that.CloudTechnologies_CRM_One == null : this.CloudTechnologies_CRM_One.equals(that.CloudTechnologies_CRM_One));
    equal = equal && (this.CloudTechnologies_CRM_Two == null ? that.CloudTechnologies_CRM_Two == null : this.CloudTechnologies_CRM_Two.equals(that.CloudTechnologies_CRM_Two));
    equal = equal && (this.CloudTechnologies_DataCenterSolutions_One == null ? that.CloudTechnologies_DataCenterSolutions_One == null : this.CloudTechnologies_DataCenterSolutions_One.equals(that.CloudTechnologies_DataCenterSolutions_One));
    equal = equal && (this.CloudTechnologies_DataCenterSolutions_Two == null ? that.CloudTechnologies_DataCenterSolutions_Two == null : this.CloudTechnologies_DataCenterSolutions_Two.equals(that.CloudTechnologies_DataCenterSolutions_Two));
    equal = equal && (this.CloudTechnologies_EnterpriseApplications_One == null ? that.CloudTechnologies_EnterpriseApplications_One == null : this.CloudTechnologies_EnterpriseApplications_One.equals(that.CloudTechnologies_EnterpriseApplications_One));
    equal = equal && (this.CloudTechnologies_EnterpriseApplications_Two == null ? that.CloudTechnologies_EnterpriseApplications_Two == null : this.CloudTechnologies_EnterpriseApplications_Two.equals(that.CloudTechnologies_EnterpriseApplications_Two));
    equal = equal && (this.CloudTechnologies_EnterpriseContent_One == null ? that.CloudTechnologies_EnterpriseContent_One == null : this.CloudTechnologies_EnterpriseContent_One.equals(that.CloudTechnologies_EnterpriseContent_One));
    equal = equal && (this.CloudTechnologies_EnterpriseContent_Two == null ? that.CloudTechnologies_EnterpriseContent_Two == null : this.CloudTechnologies_EnterpriseContent_Two.equals(that.CloudTechnologies_EnterpriseContent_Two));
    equal = equal && (this.CloudTechnologies_HardwareBasic_One == null ? that.CloudTechnologies_HardwareBasic_One == null : this.CloudTechnologies_HardwareBasic_One.equals(that.CloudTechnologies_HardwareBasic_One));
    equal = equal && (this.CloudTechnologies_HardwareBasic_Two == null ? that.CloudTechnologies_HardwareBasic_Two == null : this.CloudTechnologies_HardwareBasic_Two.equals(that.CloudTechnologies_HardwareBasic_Two));
    equal = equal && (this.CloudTechnologies_ITGovernance_One == null ? that.CloudTechnologies_ITGovernance_One == null : this.CloudTechnologies_ITGovernance_One.equals(that.CloudTechnologies_ITGovernance_One));
    equal = equal && (this.CloudTechnologies_ITGovernance_Two == null ? that.CloudTechnologies_ITGovernance_Two == null : this.CloudTechnologies_ITGovernance_Two.equals(that.CloudTechnologies_ITGovernance_Two));
    equal = equal && (this.CloudTechnologies_MarketingPerfMgmt_One == null ? that.CloudTechnologies_MarketingPerfMgmt_One == null : this.CloudTechnologies_MarketingPerfMgmt_One.equals(that.CloudTechnologies_MarketingPerfMgmt_One));
    equal = equal && (this.CloudTechnologies_MarketingPerfMgmt_Two == null ? that.CloudTechnologies_MarketingPerfMgmt_Two == null : this.CloudTechnologies_MarketingPerfMgmt_Two.equals(that.CloudTechnologies_MarketingPerfMgmt_Two));
    equal = equal && (this.CloudTechnologies_NetworkComputing_One == null ? that.CloudTechnologies_NetworkComputing_One == null : this.CloudTechnologies_NetworkComputing_One.equals(that.CloudTechnologies_NetworkComputing_One));
    equal = equal && (this.CloudTechnologies_NetworkComputing_Two == null ? that.CloudTechnologies_NetworkComputing_Two == null : this.CloudTechnologies_NetworkComputing_Two.equals(that.CloudTechnologies_NetworkComputing_Two));
    equal = equal && (this.CloudTechnologies_ProductivitySltns_One == null ? that.CloudTechnologies_ProductivitySltns_One == null : this.CloudTechnologies_ProductivitySltns_One.equals(that.CloudTechnologies_ProductivitySltns_One));
    equal = equal && (this.CloudTechnologies_ProductivitySltns_Two == null ? that.CloudTechnologies_ProductivitySltns_Two == null : this.CloudTechnologies_ProductivitySltns_Two.equals(that.CloudTechnologies_ProductivitySltns_Two));
    equal = equal && (this.CloudTechnologies_ProjectMgnt_One == null ? that.CloudTechnologies_ProjectMgnt_One == null : this.CloudTechnologies_ProjectMgnt_One.equals(that.CloudTechnologies_ProjectMgnt_One));
    equal = equal && (this.CloudTechnologies_ProjectMgnt_Two == null ? that.CloudTechnologies_ProjectMgnt_Two == null : this.CloudTechnologies_ProjectMgnt_Two.equals(that.CloudTechnologies_ProjectMgnt_Two));
    equal = equal && (this.CloudTechnologies_SoftwareBasic_One == null ? that.CloudTechnologies_SoftwareBasic_One == null : this.CloudTechnologies_SoftwareBasic_One.equals(that.CloudTechnologies_SoftwareBasic_One));
    equal = equal && (this.CloudTechnologies_SoftwareBasic_Two == null ? that.CloudTechnologies_SoftwareBasic_Two == null : this.CloudTechnologies_SoftwareBasic_Two.equals(that.CloudTechnologies_SoftwareBasic_Two));
    equal = equal && (this.CloudTechnologies_VerticalMarkets_One == null ? that.CloudTechnologies_VerticalMarkets_One == null : this.CloudTechnologies_VerticalMarkets_One.equals(that.CloudTechnologies_VerticalMarkets_One));
    equal = equal && (this.CloudTechnologies_VerticalMarkets_Two == null ? that.CloudTechnologies_VerticalMarkets_Two == null : this.CloudTechnologies_VerticalMarkets_Two.equals(that.CloudTechnologies_VerticalMarkets_Two));
    equal = equal && (this.CloudTechnologies_WebOrntdArch_One == null ? that.CloudTechnologies_WebOrntdArch_One == null : this.CloudTechnologies_WebOrntdArch_One.equals(that.CloudTechnologies_WebOrntdArch_One));
    equal = equal && (this.CloudTechnologies_WebOrntdArch_Two == null ? that.CloudTechnologies_WebOrntdArch_Two == null : this.CloudTechnologies_WebOrntdArch_Two.equals(that.CloudTechnologies_WebOrntdArch_Two));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Nutanix)) {
      return false;
    }
    Nutanix that = (Nutanix) o;
    boolean equal = true;
    equal = equal && (this.Nutanix_EventTable_Clean == null ? that.Nutanix_EventTable_Clean == null : this.Nutanix_EventTable_Clean.equals(that.Nutanix_EventTable_Clean));
    equal = equal && (this.LeadID == null ? that.LeadID == null : this.LeadID.equals(that.LeadID));
    equal = equal && (this.CustomerID == null ? that.CustomerID == null : this.CustomerID.equals(that.CustomerID));
    equal = equal && (this.PeriodID == null ? that.PeriodID == null : this.PeriodID.equals(that.PeriodID));
    equal = equal && (this.P1_Target == null ? that.P1_Target == null : this.P1_Target.equals(that.P1_Target));
    equal = equal && (this.P1_TargetTraining == null ? that.P1_TargetTraining == null : this.P1_TargetTraining.equals(that.P1_TargetTraining));
    equal = equal && (this.P1_Event == null ? that.P1_Event == null : this.P1_Event.equals(that.P1_Event));
    equal = equal && (this.Company == null ? that.Company == null : this.Company.equals(that.Company));
    equal = equal && (this.Email == null ? that.Email == null : this.Email.equals(that.Email));
    equal = equal && (this.Domain == null ? that.Domain == null : this.Domain.equals(that.Domain));
    equal = equal && (this.AwardYear == null ? that.AwardYear == null : this.AwardYear.equals(that.AwardYear));
    equal = equal && (this.BankruptcyFiled == null ? that.BankruptcyFiled == null : this.BankruptcyFiled.equals(that.BankruptcyFiled));
    equal = equal && (this.BusinessAnnualSalesAbs == null ? that.BusinessAnnualSalesAbs == null : this.BusinessAnnualSalesAbs.equals(that.BusinessAnnualSalesAbs));
    equal = equal && (this.BusinessAssets == null ? that.BusinessAssets == null : this.BusinessAssets.equals(that.BusinessAssets));
    equal = equal && (this.BusinessECommerceSite == null ? that.BusinessECommerceSite == null : this.BusinessECommerceSite.equals(that.BusinessECommerceSite));
    equal = equal && (this.BusinessEntityType == null ? that.BusinessEntityType == null : this.BusinessEntityType.equals(that.BusinessEntityType));
    equal = equal && (this.BusinessEstablishedYear == null ? that.BusinessEstablishedYear == null : this.BusinessEstablishedYear.equals(that.BusinessEstablishedYear));
    equal = equal && (this.BusinessEstimatedAnnualSales_k == null ? that.BusinessEstimatedAnnualSales_k == null : this.BusinessEstimatedAnnualSales_k.equals(that.BusinessEstimatedAnnualSales_k));
    equal = equal && (this.BusinessEstimatedEmployees == null ? that.BusinessEstimatedEmployees == null : this.BusinessEstimatedEmployees.equals(that.BusinessEstimatedEmployees));
    equal = equal && (this.BusinessFirmographicsParentEmployees == null ? that.BusinessFirmographicsParentEmployees == null : this.BusinessFirmographicsParentEmployees.equals(that.BusinessFirmographicsParentEmployees));
    equal = equal && (this.BusinessFirmographicsParentRevenue == null ? that.BusinessFirmographicsParentRevenue == null : this.BusinessFirmographicsParentRevenue.equals(that.BusinessFirmographicsParentRevenue));
    equal = equal && (this.BusinessIndustrySector == null ? that.BusinessIndustrySector == null : this.BusinessIndustrySector.equals(that.BusinessIndustrySector));
    equal = equal && (this.BusinessRetirementParticipants == null ? that.BusinessRetirementParticipants == null : this.BusinessRetirementParticipants.equals(that.BusinessRetirementParticipants));
    equal = equal && (this.BusinessSocialPresence == null ? that.BusinessSocialPresence == null : this.BusinessSocialPresence.equals(that.BusinessSocialPresence));
    equal = equal && (this.BusinessUrlNumPages == null ? that.BusinessUrlNumPages == null : this.BusinessUrlNumPages.equals(that.BusinessUrlNumPages));
    equal = equal && (this.BusinessType == null ? that.BusinessType == null : this.BusinessType.equals(that.BusinessType));
    equal = equal && (this.BusinessVCFunded == null ? that.BusinessVCFunded == null : this.BusinessVCFunded.equals(that.BusinessVCFunded));
    equal = equal && (this.DerogatoryIndicator == null ? that.DerogatoryIndicator == null : this.DerogatoryIndicator.equals(that.DerogatoryIndicator));
    equal = equal && (this.ExperianCreditRating == null ? that.ExperianCreditRating == null : this.ExperianCreditRating.equals(that.ExperianCreditRating));
    equal = equal && (this.FundingAgency == null ? that.FundingAgency == null : this.FundingAgency.equals(that.FundingAgency));
    equal = equal && (this.FundingAmount == null ? that.FundingAmount == null : this.FundingAmount.equals(that.FundingAmount));
    equal = equal && (this.FundingAwardAmount == null ? that.FundingAwardAmount == null : this.FundingAwardAmount.equals(that.FundingAwardAmount));
    equal = equal && (this.FundingFinanceRound == null ? that.FundingFinanceRound == null : this.FundingFinanceRound.equals(that.FundingFinanceRound));
    equal = equal && (this.FundingFiscalQuarter == null ? that.FundingFiscalQuarter == null : this.FundingFiscalQuarter.equals(that.FundingFiscalQuarter));
    equal = equal && (this.FundingFiscalYear == null ? that.FundingFiscalYear == null : this.FundingFiscalYear.equals(that.FundingFiscalYear));
    equal = equal && (this.FundingReceived == null ? that.FundingReceived == null : this.FundingReceived.equals(that.FundingReceived));
    equal = equal && (this.FundingStage == null ? that.FundingStage == null : this.FundingStage.equals(that.FundingStage));
    equal = equal && (this.Intelliscore == null ? that.Intelliscore == null : this.Intelliscore.equals(that.Intelliscore));
    equal = equal && (this.JobsRecentJobs == null ? that.JobsRecentJobs == null : this.JobsRecentJobs.equals(that.JobsRecentJobs));
    equal = equal && (this.JobsTrendString == null ? that.JobsTrendString == null : this.JobsTrendString.equals(that.JobsTrendString));
    equal = equal && (this.ModelAction == null ? that.ModelAction == null : this.ModelAction.equals(that.ModelAction));
    equal = equal && (this.PercentileModel == null ? that.PercentileModel == null : this.PercentileModel.equals(that.PercentileModel));
    equal = equal && (this.RetirementAssetsEOY == null ? that.RetirementAssetsEOY == null : this.RetirementAssetsEOY.equals(that.RetirementAssetsEOY));
    equal = equal && (this.RetirementAssetsYOY == null ? that.RetirementAssetsYOY == null : this.RetirementAssetsYOY.equals(that.RetirementAssetsYOY));
    equal = equal && (this.TotalParticipantsSOY == null ? that.TotalParticipantsSOY == null : this.TotalParticipantsSOY.equals(that.TotalParticipantsSOY));
    equal = equal && (this.UCCFilings == null ? that.UCCFilings == null : this.UCCFilings.equals(that.UCCFilings));
    equal = equal && (this.UCCFilingsPresent == null ? that.UCCFilingsPresent == null : this.UCCFilingsPresent.equals(that.UCCFilingsPresent));
    equal = equal && (this.Years_in_Business_Code == null ? that.Years_in_Business_Code == null : this.Years_in_Business_Code.equals(that.Years_in_Business_Code));
    equal = equal && (this.Non_Profit_Indicator == null ? that.Non_Profit_Indicator == null : this.Non_Profit_Indicator.equals(that.Non_Profit_Indicator));
    equal = equal && (this.PD_DA_AwardCategory == null ? that.PD_DA_AwardCategory == null : this.PD_DA_AwardCategory.equals(that.PD_DA_AwardCategory));
    equal = equal && (this.PD_DA_JobTitle == null ? that.PD_DA_JobTitle == null : this.PD_DA_JobTitle.equals(that.PD_DA_JobTitle));
    equal = equal && (this.PD_DA_LastSocialActivity_Units == null ? that.PD_DA_LastSocialActivity_Units == null : this.PD_DA_LastSocialActivity_Units.equals(that.PD_DA_LastSocialActivity_Units));
    equal = equal && (this.PD_DA_MonthsPatentGranted == null ? that.PD_DA_MonthsPatentGranted == null : this.PD_DA_MonthsPatentGranted.equals(that.PD_DA_MonthsPatentGranted));
    equal = equal && (this.PD_DA_MonthsSinceFundAwardDate == null ? that.PD_DA_MonthsSinceFundAwardDate == null : this.PD_DA_MonthsSinceFundAwardDate.equals(that.PD_DA_MonthsSinceFundAwardDate));
    equal = equal && (this.PD_DA_PrimarySIC1 == null ? that.PD_DA_PrimarySIC1 == null : this.PD_DA_PrimarySIC1.equals(that.PD_DA_PrimarySIC1));
    equal = equal && (this.Industry == null ? that.Industry == null : this.Industry.equals(that.Industry));
    equal = equal && (this.LeadSource == null ? that.LeadSource == null : this.LeadSource.equals(that.LeadSource));
    equal = equal && (this.AnnualRevenue == null ? that.AnnualRevenue == null : this.AnnualRevenue.equals(that.AnnualRevenue));
    equal = equal && (this.NumberOfEmployees == null ? that.NumberOfEmployees == null : this.NumberOfEmployees.equals(that.NumberOfEmployees));
    equal = equal && (this.Alexa_MonthsSinceOnline == null ? that.Alexa_MonthsSinceOnline == null : this.Alexa_MonthsSinceOnline.equals(that.Alexa_MonthsSinceOnline));
    equal = equal && (this.Alexa_Rank == null ? that.Alexa_Rank == null : this.Alexa_Rank.equals(that.Alexa_Rank));
    equal = equal && (this.Alexa_ReachPerMillion == null ? that.Alexa_ReachPerMillion == null : this.Alexa_ReachPerMillion.equals(that.Alexa_ReachPerMillion));
    equal = equal && (this.Alexa_ViewsPerMillion == null ? that.Alexa_ViewsPerMillion == null : this.Alexa_ViewsPerMillion.equals(that.Alexa_ViewsPerMillion));
    equal = equal && (this.Alexa_ViewsPerUser == null ? that.Alexa_ViewsPerUser == null : this.Alexa_ViewsPerUser.equals(that.Alexa_ViewsPerUser));
    equal = equal && (this.BW_TechTags_Cnt == null ? that.BW_TechTags_Cnt == null : this.BW_TechTags_Cnt.equals(that.BW_TechTags_Cnt));
    equal = equal && (this.BW_TotalTech_Cnt == null ? that.BW_TotalTech_Cnt == null : this.BW_TotalTech_Cnt.equals(that.BW_TotalTech_Cnt));
    equal = equal && (this.BW_ads == null ? that.BW_ads == null : this.BW_ads.equals(that.BW_ads));
    equal = equal && (this.BW_analytics == null ? that.BW_analytics == null : this.BW_analytics.equals(that.BW_analytics));
    equal = equal && (this.BW_cdn == null ? that.BW_cdn == null : this.BW_cdn.equals(that.BW_cdn));
    equal = equal && (this.BW_cdns == null ? that.BW_cdns == null : this.BW_cdns.equals(that.BW_cdns));
    equal = equal && (this.BW_cms == null ? that.BW_cms == null : this.BW_cms.equals(that.BW_cms));
    equal = equal && (this.BW_docinfo == null ? that.BW_docinfo == null : this.BW_docinfo.equals(that.BW_docinfo));
    equal = equal && (this.BW_encoding == null ? that.BW_encoding == null : this.BW_encoding.equals(that.BW_encoding));
    equal = equal && (this.BW_feeds == null ? that.BW_feeds == null : this.BW_feeds.equals(that.BW_feeds));
    equal = equal && (this.BW_framework == null ? that.BW_framework == null : this.BW_framework.equals(that.BW_framework));
    equal = equal && (this.BW_hosting == null ? that.BW_hosting == null : this.BW_hosting.equals(that.BW_hosting));
    equal = equal && (this.BW_javascript == null ? that.BW_javascript == null : this.BW_javascript.equals(that.BW_javascript));
    equal = equal && (this.BW_mapping == null ? that.BW_mapping == null : this.BW_mapping.equals(that.BW_mapping));
    equal = equal && (this.BW_media == null ? that.BW_media == null : this.BW_media.equals(that.BW_media));
    equal = equal && (this.BW_mx == null ? that.BW_mx == null : this.BW_mx.equals(that.BW_mx));
    equal = equal && (this.BW_ns == null ? that.BW_ns == null : this.BW_ns.equals(that.BW_ns));
    equal = equal && (this.BW_parked == null ? that.BW_parked == null : this.BW_parked.equals(that.BW_parked));
    equal = equal && (this.BW_payment == null ? that.BW_payment == null : this.BW_payment.equals(that.BW_payment));
    equal = equal && (this.BW_seo_headers == null ? that.BW_seo_headers == null : this.BW_seo_headers.equals(that.BW_seo_headers));
    equal = equal && (this.BW_seo_meta == null ? that.BW_seo_meta == null : this.BW_seo_meta.equals(that.BW_seo_meta));
    equal = equal && (this.BW_seo_title == null ? that.BW_seo_title == null : this.BW_seo_title.equals(that.BW_seo_title));
    equal = equal && (this.BW_Server == null ? that.BW_Server == null : this.BW_Server.equals(that.BW_Server));
    equal = equal && (this.BW_shop == null ? that.BW_shop == null : this.BW_shop.equals(that.BW_shop));
    equal = equal && (this.BW_ssl == null ? that.BW_ssl == null : this.BW_ssl.equals(that.BW_ssl));
    equal = equal && (this.BW_Web_Master == null ? that.BW_Web_Master == null : this.BW_Web_Master.equals(that.BW_Web_Master));
    equal = equal && (this.BW_Web_Server == null ? that.BW_Web_Server == null : this.BW_Web_Server.equals(that.BW_Web_Server));
    equal = equal && (this.BW_widgets == null ? that.BW_widgets == null : this.BW_widgets.equals(that.BW_widgets));
    equal = equal && (this.Activity_ClickLink_cnt == null ? that.Activity_ClickLink_cnt == null : this.Activity_ClickLink_cnt.equals(that.Activity_ClickLink_cnt));
    equal = equal && (this.Activity_VisitWeb_cnt == null ? that.Activity_VisitWeb_cnt == null : this.Activity_VisitWeb_cnt.equals(that.Activity_VisitWeb_cnt));
    equal = equal && (this.Activity_InterestingMoment_cnt == null ? that.Activity_InterestingMoment_cnt == null : this.Activity_InterestingMoment_cnt.equals(that.Activity_InterestingMoment_cnt));
    equal = equal && (this.Activity_OpenEmail_cnt == null ? that.Activity_OpenEmail_cnt == null : this.Activity_OpenEmail_cnt.equals(that.Activity_OpenEmail_cnt));
    equal = equal && (this.Activity_EmailBncedSft_cnt == null ? that.Activity_EmailBncedSft_cnt == null : this.Activity_EmailBncedSft_cnt.equals(that.Activity_EmailBncedSft_cnt));
    equal = equal && (this.Activity_FillOutForm_cnt == null ? that.Activity_FillOutForm_cnt == null : this.Activity_FillOutForm_cnt.equals(that.Activity_FillOutForm_cnt));
    equal = equal && (this.Activity_UnsubscrbEmail_cnt == null ? that.Activity_UnsubscrbEmail_cnt == null : this.Activity_UnsubscrbEmail_cnt.equals(that.Activity_UnsubscrbEmail_cnt));
    equal = equal && (this.Activity_ClickEmail_cnt == null ? that.Activity_ClickEmail_cnt == null : this.Activity_ClickEmail_cnt.equals(that.Activity_ClickEmail_cnt));
    equal = equal && (this.CloudTechnologies_CloudService_One == null ? that.CloudTechnologies_CloudService_One == null : this.CloudTechnologies_CloudService_One.equals(that.CloudTechnologies_CloudService_One));
    equal = equal && (this.CloudTechnologies_CloudService_Two == null ? that.CloudTechnologies_CloudService_Two == null : this.CloudTechnologies_CloudService_Two.equals(that.CloudTechnologies_CloudService_Two));
    equal = equal && (this.CloudTechnologies_CommTech_One == null ? that.CloudTechnologies_CommTech_One == null : this.CloudTechnologies_CommTech_One.equals(that.CloudTechnologies_CommTech_One));
    equal = equal && (this.CloudTechnologies_CommTech_Two == null ? that.CloudTechnologies_CommTech_Two == null : this.CloudTechnologies_CommTech_Two.equals(that.CloudTechnologies_CommTech_Two));
    equal = equal && (this.CloudTechnologies_CRM_One == null ? that.CloudTechnologies_CRM_One == null : this.CloudTechnologies_CRM_One.equals(that.CloudTechnologies_CRM_One));
    equal = equal && (this.CloudTechnologies_CRM_Two == null ? that.CloudTechnologies_CRM_Two == null : this.CloudTechnologies_CRM_Two.equals(that.CloudTechnologies_CRM_Two));
    equal = equal && (this.CloudTechnologies_DataCenterSolutions_One == null ? that.CloudTechnologies_DataCenterSolutions_One == null : this.CloudTechnologies_DataCenterSolutions_One.equals(that.CloudTechnologies_DataCenterSolutions_One));
    equal = equal && (this.CloudTechnologies_DataCenterSolutions_Two == null ? that.CloudTechnologies_DataCenterSolutions_Two == null : this.CloudTechnologies_DataCenterSolutions_Two.equals(that.CloudTechnologies_DataCenterSolutions_Two));
    equal = equal && (this.CloudTechnologies_EnterpriseApplications_One == null ? that.CloudTechnologies_EnterpriseApplications_One == null : this.CloudTechnologies_EnterpriseApplications_One.equals(that.CloudTechnologies_EnterpriseApplications_One));
    equal = equal && (this.CloudTechnologies_EnterpriseApplications_Two == null ? that.CloudTechnologies_EnterpriseApplications_Two == null : this.CloudTechnologies_EnterpriseApplications_Two.equals(that.CloudTechnologies_EnterpriseApplications_Two));
    equal = equal && (this.CloudTechnologies_EnterpriseContent_One == null ? that.CloudTechnologies_EnterpriseContent_One == null : this.CloudTechnologies_EnterpriseContent_One.equals(that.CloudTechnologies_EnterpriseContent_One));
    equal = equal && (this.CloudTechnologies_EnterpriseContent_Two == null ? that.CloudTechnologies_EnterpriseContent_Two == null : this.CloudTechnologies_EnterpriseContent_Two.equals(that.CloudTechnologies_EnterpriseContent_Two));
    equal = equal && (this.CloudTechnologies_HardwareBasic_One == null ? that.CloudTechnologies_HardwareBasic_One == null : this.CloudTechnologies_HardwareBasic_One.equals(that.CloudTechnologies_HardwareBasic_One));
    equal = equal && (this.CloudTechnologies_HardwareBasic_Two == null ? that.CloudTechnologies_HardwareBasic_Two == null : this.CloudTechnologies_HardwareBasic_Two.equals(that.CloudTechnologies_HardwareBasic_Two));
    equal = equal && (this.CloudTechnologies_ITGovernance_One == null ? that.CloudTechnologies_ITGovernance_One == null : this.CloudTechnologies_ITGovernance_One.equals(that.CloudTechnologies_ITGovernance_One));
    equal = equal && (this.CloudTechnologies_ITGovernance_Two == null ? that.CloudTechnologies_ITGovernance_Two == null : this.CloudTechnologies_ITGovernance_Two.equals(that.CloudTechnologies_ITGovernance_Two));
    equal = equal && (this.CloudTechnologies_MarketingPerfMgmt_One == null ? that.CloudTechnologies_MarketingPerfMgmt_One == null : this.CloudTechnologies_MarketingPerfMgmt_One.equals(that.CloudTechnologies_MarketingPerfMgmt_One));
    equal = equal && (this.CloudTechnologies_MarketingPerfMgmt_Two == null ? that.CloudTechnologies_MarketingPerfMgmt_Two == null : this.CloudTechnologies_MarketingPerfMgmt_Two.equals(that.CloudTechnologies_MarketingPerfMgmt_Two));
    equal = equal && (this.CloudTechnologies_NetworkComputing_One == null ? that.CloudTechnologies_NetworkComputing_One == null : this.CloudTechnologies_NetworkComputing_One.equals(that.CloudTechnologies_NetworkComputing_One));
    equal = equal && (this.CloudTechnologies_NetworkComputing_Two == null ? that.CloudTechnologies_NetworkComputing_Two == null : this.CloudTechnologies_NetworkComputing_Two.equals(that.CloudTechnologies_NetworkComputing_Two));
    equal = equal && (this.CloudTechnologies_ProductivitySltns_One == null ? that.CloudTechnologies_ProductivitySltns_One == null : this.CloudTechnologies_ProductivitySltns_One.equals(that.CloudTechnologies_ProductivitySltns_One));
    equal = equal && (this.CloudTechnologies_ProductivitySltns_Two == null ? that.CloudTechnologies_ProductivitySltns_Two == null : this.CloudTechnologies_ProductivitySltns_Two.equals(that.CloudTechnologies_ProductivitySltns_Two));
    equal = equal && (this.CloudTechnologies_ProjectMgnt_One == null ? that.CloudTechnologies_ProjectMgnt_One == null : this.CloudTechnologies_ProjectMgnt_One.equals(that.CloudTechnologies_ProjectMgnt_One));
    equal = equal && (this.CloudTechnologies_ProjectMgnt_Two == null ? that.CloudTechnologies_ProjectMgnt_Two == null : this.CloudTechnologies_ProjectMgnt_Two.equals(that.CloudTechnologies_ProjectMgnt_Two));
    equal = equal && (this.CloudTechnologies_SoftwareBasic_One == null ? that.CloudTechnologies_SoftwareBasic_One == null : this.CloudTechnologies_SoftwareBasic_One.equals(that.CloudTechnologies_SoftwareBasic_One));
    equal = equal && (this.CloudTechnologies_SoftwareBasic_Two == null ? that.CloudTechnologies_SoftwareBasic_Two == null : this.CloudTechnologies_SoftwareBasic_Two.equals(that.CloudTechnologies_SoftwareBasic_Two));
    equal = equal && (this.CloudTechnologies_VerticalMarkets_One == null ? that.CloudTechnologies_VerticalMarkets_One == null : this.CloudTechnologies_VerticalMarkets_One.equals(that.CloudTechnologies_VerticalMarkets_One));
    equal = equal && (this.CloudTechnologies_VerticalMarkets_Two == null ? that.CloudTechnologies_VerticalMarkets_Two == null : this.CloudTechnologies_VerticalMarkets_Two.equals(that.CloudTechnologies_VerticalMarkets_Two));
    equal = equal && (this.CloudTechnologies_WebOrntdArch_One == null ? that.CloudTechnologies_WebOrntdArch_One == null : this.CloudTechnologies_WebOrntdArch_One.equals(that.CloudTechnologies_WebOrntdArch_One));
    equal = equal && (this.CloudTechnologies_WebOrntdArch_Two == null ? that.CloudTechnologies_WebOrntdArch_Two == null : this.CloudTechnologies_WebOrntdArch_Two.equals(that.CloudTechnologies_WebOrntdArch_Two));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.Nutanix_EventTable_Clean = JdbcWritableBridge.readLong(1, __dbResults);
    this.LeadID = JdbcWritableBridge.readString(2, __dbResults);
    this.CustomerID = JdbcWritableBridge.readString(3, __dbResults);
    this.PeriodID = JdbcWritableBridge.readLong(4, __dbResults);
    this.P1_Target = JdbcWritableBridge.readLong(5, __dbResults);
    this.P1_TargetTraining = JdbcWritableBridge.readLong(6, __dbResults);
    this.P1_Event = JdbcWritableBridge.readString(7, __dbResults);
    this.Company = JdbcWritableBridge.readString(8, __dbResults);
    this.Email = JdbcWritableBridge.readString(9, __dbResults);
    this.Domain = JdbcWritableBridge.readString(10, __dbResults);
    this.AwardYear = JdbcWritableBridge.readLong(11, __dbResults);
    this.BankruptcyFiled = JdbcWritableBridge.readString(12, __dbResults);
    this.BusinessAnnualSalesAbs = JdbcWritableBridge.readDouble(13, __dbResults);
    this.BusinessAssets = JdbcWritableBridge.readString(14, __dbResults);
    this.BusinessECommerceSite = JdbcWritableBridge.readString(15, __dbResults);
    this.BusinessEntityType = JdbcWritableBridge.readString(16, __dbResults);
    this.BusinessEstablishedYear = JdbcWritableBridge.readDouble(17, __dbResults);
    this.BusinessEstimatedAnnualSales_k = JdbcWritableBridge.readDouble(18, __dbResults);
    this.BusinessEstimatedEmployees = JdbcWritableBridge.readDouble(19, __dbResults);
    this.BusinessFirmographicsParentEmployees = JdbcWritableBridge.readDouble(20, __dbResults);
    this.BusinessFirmographicsParentRevenue = JdbcWritableBridge.readDouble(21, __dbResults);
    this.BusinessIndustrySector = JdbcWritableBridge.readString(22, __dbResults);
    this.BusinessRetirementParticipants = JdbcWritableBridge.readDouble(23, __dbResults);
    this.BusinessSocialPresence = JdbcWritableBridge.readString(24, __dbResults);
    this.BusinessUrlNumPages = JdbcWritableBridge.readDouble(25, __dbResults);
    this.BusinessType = JdbcWritableBridge.readString(26, __dbResults);
    this.BusinessVCFunded = JdbcWritableBridge.readString(27, __dbResults);
    this.DerogatoryIndicator = JdbcWritableBridge.readString(28, __dbResults);
    this.ExperianCreditRating = JdbcWritableBridge.readString(29, __dbResults);
    this.FundingAgency = JdbcWritableBridge.readString(30, __dbResults);
    this.FundingAmount = JdbcWritableBridge.readDouble(31, __dbResults);
    this.FundingAwardAmount = JdbcWritableBridge.readDouble(32, __dbResults);
    this.FundingFinanceRound = JdbcWritableBridge.readDouble(33, __dbResults);
    this.FundingFiscalQuarter = JdbcWritableBridge.readDouble(34, __dbResults);
    this.FundingFiscalYear = JdbcWritableBridge.readDouble(35, __dbResults);
    this.FundingReceived = JdbcWritableBridge.readDouble(36, __dbResults);
    this.FundingStage = JdbcWritableBridge.readString(37, __dbResults);
    this.Intelliscore = JdbcWritableBridge.readDouble(38, __dbResults);
    this.JobsRecentJobs = JdbcWritableBridge.readDouble(39, __dbResults);
    this.JobsTrendString = JdbcWritableBridge.readString(40, __dbResults);
    this.ModelAction = JdbcWritableBridge.readString(41, __dbResults);
    this.PercentileModel = JdbcWritableBridge.readDouble(42, __dbResults);
    this.RetirementAssetsEOY = JdbcWritableBridge.readDouble(43, __dbResults);
    this.RetirementAssetsYOY = JdbcWritableBridge.readDouble(44, __dbResults);
    this.TotalParticipantsSOY = JdbcWritableBridge.readDouble(45, __dbResults);
    this.UCCFilings = JdbcWritableBridge.readDouble(46, __dbResults);
    this.UCCFilingsPresent = JdbcWritableBridge.readString(47, __dbResults);
    this.Years_in_Business_Code = JdbcWritableBridge.readString(48, __dbResults);
    this.Non_Profit_Indicator = JdbcWritableBridge.readString(49, __dbResults);
    this.PD_DA_AwardCategory = JdbcWritableBridge.readString(50, __dbResults);
    this.PD_DA_JobTitle = JdbcWritableBridge.readString(51, __dbResults);
    this.PD_DA_LastSocialActivity_Units = JdbcWritableBridge.readString(52, __dbResults);
    this.PD_DA_MonthsPatentGranted = JdbcWritableBridge.readDouble(53, __dbResults);
    this.PD_DA_MonthsSinceFundAwardDate = JdbcWritableBridge.readDouble(54, __dbResults);
    this.PD_DA_PrimarySIC1 = JdbcWritableBridge.readString(55, __dbResults);
    this.Industry = JdbcWritableBridge.readString(56, __dbResults);
    this.LeadSource = JdbcWritableBridge.readString(57, __dbResults);
    this.AnnualRevenue = JdbcWritableBridge.readDouble(58, __dbResults);
    this.NumberOfEmployees = JdbcWritableBridge.readDouble(59, __dbResults);
    this.Alexa_MonthsSinceOnline = JdbcWritableBridge.readDouble(60, __dbResults);
    this.Alexa_Rank = JdbcWritableBridge.readDouble(61, __dbResults);
    this.Alexa_ReachPerMillion = JdbcWritableBridge.readDouble(62, __dbResults);
    this.Alexa_ViewsPerMillion = JdbcWritableBridge.readDouble(63, __dbResults);
    this.Alexa_ViewsPerUser = JdbcWritableBridge.readDouble(64, __dbResults);
    this.BW_TechTags_Cnt = JdbcWritableBridge.readDouble(65, __dbResults);
    this.BW_TotalTech_Cnt = JdbcWritableBridge.readDouble(66, __dbResults);
    this.BW_ads = JdbcWritableBridge.readDouble(67, __dbResults);
    this.BW_analytics = JdbcWritableBridge.readDouble(68, __dbResults);
    this.BW_cdn = JdbcWritableBridge.readDouble(69, __dbResults);
    this.BW_cdns = JdbcWritableBridge.readDouble(70, __dbResults);
    this.BW_cms = JdbcWritableBridge.readDouble(71, __dbResults);
    this.BW_docinfo = JdbcWritableBridge.readDouble(72, __dbResults);
    this.BW_encoding = JdbcWritableBridge.readDouble(73, __dbResults);
    this.BW_feeds = JdbcWritableBridge.readDouble(74, __dbResults);
    this.BW_framework = JdbcWritableBridge.readDouble(75, __dbResults);
    this.BW_hosting = JdbcWritableBridge.readDouble(76, __dbResults);
    this.BW_javascript = JdbcWritableBridge.readDouble(77, __dbResults);
    this.BW_mapping = JdbcWritableBridge.readDouble(78, __dbResults);
    this.BW_media = JdbcWritableBridge.readDouble(79, __dbResults);
    this.BW_mx = JdbcWritableBridge.readDouble(80, __dbResults);
    this.BW_ns = JdbcWritableBridge.readDouble(81, __dbResults);
    this.BW_parked = JdbcWritableBridge.readDouble(82, __dbResults);
    this.BW_payment = JdbcWritableBridge.readDouble(83, __dbResults);
    this.BW_seo_headers = JdbcWritableBridge.readDouble(84, __dbResults);
    this.BW_seo_meta = JdbcWritableBridge.readDouble(85, __dbResults);
    this.BW_seo_title = JdbcWritableBridge.readDouble(86, __dbResults);
    this.BW_Server = JdbcWritableBridge.readDouble(87, __dbResults);
    this.BW_shop = JdbcWritableBridge.readDouble(88, __dbResults);
    this.BW_ssl = JdbcWritableBridge.readDouble(89, __dbResults);
    this.BW_Web_Master = JdbcWritableBridge.readDouble(90, __dbResults);
    this.BW_Web_Server = JdbcWritableBridge.readDouble(91, __dbResults);
    this.BW_widgets = JdbcWritableBridge.readDouble(92, __dbResults);
    this.Activity_ClickLink_cnt = JdbcWritableBridge.readDouble(93, __dbResults);
    this.Activity_VisitWeb_cnt = JdbcWritableBridge.readDouble(94, __dbResults);
    this.Activity_InterestingMoment_cnt = JdbcWritableBridge.readDouble(95, __dbResults);
    this.Activity_OpenEmail_cnt = JdbcWritableBridge.readDouble(96, __dbResults);
    this.Activity_EmailBncedSft_cnt = JdbcWritableBridge.readDouble(97, __dbResults);
    this.Activity_FillOutForm_cnt = JdbcWritableBridge.readDouble(98, __dbResults);
    this.Activity_UnsubscrbEmail_cnt = JdbcWritableBridge.readDouble(99, __dbResults);
    this.Activity_ClickEmail_cnt = JdbcWritableBridge.readDouble(100, __dbResults);
    this.CloudTechnologies_CloudService_One = JdbcWritableBridge.readDouble(101, __dbResults);
    this.CloudTechnologies_CloudService_Two = JdbcWritableBridge.readDouble(102, __dbResults);
    this.CloudTechnologies_CommTech_One = JdbcWritableBridge.readDouble(103, __dbResults);
    this.CloudTechnologies_CommTech_Two = JdbcWritableBridge.readDouble(104, __dbResults);
    this.CloudTechnologies_CRM_One = JdbcWritableBridge.readDouble(105, __dbResults);
    this.CloudTechnologies_CRM_Two = JdbcWritableBridge.readDouble(106, __dbResults);
    this.CloudTechnologies_DataCenterSolutions_One = JdbcWritableBridge.readDouble(107, __dbResults);
    this.CloudTechnologies_DataCenterSolutions_Two = JdbcWritableBridge.readDouble(108, __dbResults);
    this.CloudTechnologies_EnterpriseApplications_One = JdbcWritableBridge.readDouble(109, __dbResults);
    this.CloudTechnologies_EnterpriseApplications_Two = JdbcWritableBridge.readDouble(110, __dbResults);
    this.CloudTechnologies_EnterpriseContent_One = JdbcWritableBridge.readDouble(111, __dbResults);
    this.CloudTechnologies_EnterpriseContent_Two = JdbcWritableBridge.readDouble(112, __dbResults);
    this.CloudTechnologies_HardwareBasic_One = JdbcWritableBridge.readDouble(113, __dbResults);
    this.CloudTechnologies_HardwareBasic_Two = JdbcWritableBridge.readDouble(114, __dbResults);
    this.CloudTechnologies_ITGovernance_One = JdbcWritableBridge.readDouble(115, __dbResults);
    this.CloudTechnologies_ITGovernance_Two = JdbcWritableBridge.readDouble(116, __dbResults);
    this.CloudTechnologies_MarketingPerfMgmt_One = JdbcWritableBridge.readDouble(117, __dbResults);
    this.CloudTechnologies_MarketingPerfMgmt_Two = JdbcWritableBridge.readDouble(118, __dbResults);
    this.CloudTechnologies_NetworkComputing_One = JdbcWritableBridge.readDouble(119, __dbResults);
    this.CloudTechnologies_NetworkComputing_Two = JdbcWritableBridge.readDouble(120, __dbResults);
    this.CloudTechnologies_ProductivitySltns_One = JdbcWritableBridge.readDouble(121, __dbResults);
    this.CloudTechnologies_ProductivitySltns_Two = JdbcWritableBridge.readDouble(122, __dbResults);
    this.CloudTechnologies_ProjectMgnt_One = JdbcWritableBridge.readDouble(123, __dbResults);
    this.CloudTechnologies_ProjectMgnt_Two = JdbcWritableBridge.readDouble(124, __dbResults);
    this.CloudTechnologies_SoftwareBasic_One = JdbcWritableBridge.readDouble(125, __dbResults);
    this.CloudTechnologies_SoftwareBasic_Two = JdbcWritableBridge.readDouble(126, __dbResults);
    this.CloudTechnologies_VerticalMarkets_One = JdbcWritableBridge.readDouble(127, __dbResults);
    this.CloudTechnologies_VerticalMarkets_Two = JdbcWritableBridge.readDouble(128, __dbResults);
    this.CloudTechnologies_WebOrntdArch_One = JdbcWritableBridge.readDouble(129, __dbResults);
    this.CloudTechnologies_WebOrntdArch_Two = JdbcWritableBridge.readDouble(130, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.Nutanix_EventTable_Clean = JdbcWritableBridge.readLong(1, __dbResults);
    this.LeadID = JdbcWritableBridge.readString(2, __dbResults);
    this.CustomerID = JdbcWritableBridge.readString(3, __dbResults);
    this.PeriodID = JdbcWritableBridge.readLong(4, __dbResults);
    this.P1_Target = JdbcWritableBridge.readLong(5, __dbResults);
    this.P1_TargetTraining = JdbcWritableBridge.readLong(6, __dbResults);
    this.P1_Event = JdbcWritableBridge.readString(7, __dbResults);
    this.Company = JdbcWritableBridge.readString(8, __dbResults);
    this.Email = JdbcWritableBridge.readString(9, __dbResults);
    this.Domain = JdbcWritableBridge.readString(10, __dbResults);
    this.AwardYear = JdbcWritableBridge.readLong(11, __dbResults);
    this.BankruptcyFiled = JdbcWritableBridge.readString(12, __dbResults);
    this.BusinessAnnualSalesAbs = JdbcWritableBridge.readDouble(13, __dbResults);
    this.BusinessAssets = JdbcWritableBridge.readString(14, __dbResults);
    this.BusinessECommerceSite = JdbcWritableBridge.readString(15, __dbResults);
    this.BusinessEntityType = JdbcWritableBridge.readString(16, __dbResults);
    this.BusinessEstablishedYear = JdbcWritableBridge.readDouble(17, __dbResults);
    this.BusinessEstimatedAnnualSales_k = JdbcWritableBridge.readDouble(18, __dbResults);
    this.BusinessEstimatedEmployees = JdbcWritableBridge.readDouble(19, __dbResults);
    this.BusinessFirmographicsParentEmployees = JdbcWritableBridge.readDouble(20, __dbResults);
    this.BusinessFirmographicsParentRevenue = JdbcWritableBridge.readDouble(21, __dbResults);
    this.BusinessIndustrySector = JdbcWritableBridge.readString(22, __dbResults);
    this.BusinessRetirementParticipants = JdbcWritableBridge.readDouble(23, __dbResults);
    this.BusinessSocialPresence = JdbcWritableBridge.readString(24, __dbResults);
    this.BusinessUrlNumPages = JdbcWritableBridge.readDouble(25, __dbResults);
    this.BusinessType = JdbcWritableBridge.readString(26, __dbResults);
    this.BusinessVCFunded = JdbcWritableBridge.readString(27, __dbResults);
    this.DerogatoryIndicator = JdbcWritableBridge.readString(28, __dbResults);
    this.ExperianCreditRating = JdbcWritableBridge.readString(29, __dbResults);
    this.FundingAgency = JdbcWritableBridge.readString(30, __dbResults);
    this.FundingAmount = JdbcWritableBridge.readDouble(31, __dbResults);
    this.FundingAwardAmount = JdbcWritableBridge.readDouble(32, __dbResults);
    this.FundingFinanceRound = JdbcWritableBridge.readDouble(33, __dbResults);
    this.FundingFiscalQuarter = JdbcWritableBridge.readDouble(34, __dbResults);
    this.FundingFiscalYear = JdbcWritableBridge.readDouble(35, __dbResults);
    this.FundingReceived = JdbcWritableBridge.readDouble(36, __dbResults);
    this.FundingStage = JdbcWritableBridge.readString(37, __dbResults);
    this.Intelliscore = JdbcWritableBridge.readDouble(38, __dbResults);
    this.JobsRecentJobs = JdbcWritableBridge.readDouble(39, __dbResults);
    this.JobsTrendString = JdbcWritableBridge.readString(40, __dbResults);
    this.ModelAction = JdbcWritableBridge.readString(41, __dbResults);
    this.PercentileModel = JdbcWritableBridge.readDouble(42, __dbResults);
    this.RetirementAssetsEOY = JdbcWritableBridge.readDouble(43, __dbResults);
    this.RetirementAssetsYOY = JdbcWritableBridge.readDouble(44, __dbResults);
    this.TotalParticipantsSOY = JdbcWritableBridge.readDouble(45, __dbResults);
    this.UCCFilings = JdbcWritableBridge.readDouble(46, __dbResults);
    this.UCCFilingsPresent = JdbcWritableBridge.readString(47, __dbResults);
    this.Years_in_Business_Code = JdbcWritableBridge.readString(48, __dbResults);
    this.Non_Profit_Indicator = JdbcWritableBridge.readString(49, __dbResults);
    this.PD_DA_AwardCategory = JdbcWritableBridge.readString(50, __dbResults);
    this.PD_DA_JobTitle = JdbcWritableBridge.readString(51, __dbResults);
    this.PD_DA_LastSocialActivity_Units = JdbcWritableBridge.readString(52, __dbResults);
    this.PD_DA_MonthsPatentGranted = JdbcWritableBridge.readDouble(53, __dbResults);
    this.PD_DA_MonthsSinceFundAwardDate = JdbcWritableBridge.readDouble(54, __dbResults);
    this.PD_DA_PrimarySIC1 = JdbcWritableBridge.readString(55, __dbResults);
    this.Industry = JdbcWritableBridge.readString(56, __dbResults);
    this.LeadSource = JdbcWritableBridge.readString(57, __dbResults);
    this.AnnualRevenue = JdbcWritableBridge.readDouble(58, __dbResults);
    this.NumberOfEmployees = JdbcWritableBridge.readDouble(59, __dbResults);
    this.Alexa_MonthsSinceOnline = JdbcWritableBridge.readDouble(60, __dbResults);
    this.Alexa_Rank = JdbcWritableBridge.readDouble(61, __dbResults);
    this.Alexa_ReachPerMillion = JdbcWritableBridge.readDouble(62, __dbResults);
    this.Alexa_ViewsPerMillion = JdbcWritableBridge.readDouble(63, __dbResults);
    this.Alexa_ViewsPerUser = JdbcWritableBridge.readDouble(64, __dbResults);
    this.BW_TechTags_Cnt = JdbcWritableBridge.readDouble(65, __dbResults);
    this.BW_TotalTech_Cnt = JdbcWritableBridge.readDouble(66, __dbResults);
    this.BW_ads = JdbcWritableBridge.readDouble(67, __dbResults);
    this.BW_analytics = JdbcWritableBridge.readDouble(68, __dbResults);
    this.BW_cdn = JdbcWritableBridge.readDouble(69, __dbResults);
    this.BW_cdns = JdbcWritableBridge.readDouble(70, __dbResults);
    this.BW_cms = JdbcWritableBridge.readDouble(71, __dbResults);
    this.BW_docinfo = JdbcWritableBridge.readDouble(72, __dbResults);
    this.BW_encoding = JdbcWritableBridge.readDouble(73, __dbResults);
    this.BW_feeds = JdbcWritableBridge.readDouble(74, __dbResults);
    this.BW_framework = JdbcWritableBridge.readDouble(75, __dbResults);
    this.BW_hosting = JdbcWritableBridge.readDouble(76, __dbResults);
    this.BW_javascript = JdbcWritableBridge.readDouble(77, __dbResults);
    this.BW_mapping = JdbcWritableBridge.readDouble(78, __dbResults);
    this.BW_media = JdbcWritableBridge.readDouble(79, __dbResults);
    this.BW_mx = JdbcWritableBridge.readDouble(80, __dbResults);
    this.BW_ns = JdbcWritableBridge.readDouble(81, __dbResults);
    this.BW_parked = JdbcWritableBridge.readDouble(82, __dbResults);
    this.BW_payment = JdbcWritableBridge.readDouble(83, __dbResults);
    this.BW_seo_headers = JdbcWritableBridge.readDouble(84, __dbResults);
    this.BW_seo_meta = JdbcWritableBridge.readDouble(85, __dbResults);
    this.BW_seo_title = JdbcWritableBridge.readDouble(86, __dbResults);
    this.BW_Server = JdbcWritableBridge.readDouble(87, __dbResults);
    this.BW_shop = JdbcWritableBridge.readDouble(88, __dbResults);
    this.BW_ssl = JdbcWritableBridge.readDouble(89, __dbResults);
    this.BW_Web_Master = JdbcWritableBridge.readDouble(90, __dbResults);
    this.BW_Web_Server = JdbcWritableBridge.readDouble(91, __dbResults);
    this.BW_widgets = JdbcWritableBridge.readDouble(92, __dbResults);
    this.Activity_ClickLink_cnt = JdbcWritableBridge.readDouble(93, __dbResults);
    this.Activity_VisitWeb_cnt = JdbcWritableBridge.readDouble(94, __dbResults);
    this.Activity_InterestingMoment_cnt = JdbcWritableBridge.readDouble(95, __dbResults);
    this.Activity_OpenEmail_cnt = JdbcWritableBridge.readDouble(96, __dbResults);
    this.Activity_EmailBncedSft_cnt = JdbcWritableBridge.readDouble(97, __dbResults);
    this.Activity_FillOutForm_cnt = JdbcWritableBridge.readDouble(98, __dbResults);
    this.Activity_UnsubscrbEmail_cnt = JdbcWritableBridge.readDouble(99, __dbResults);
    this.Activity_ClickEmail_cnt = JdbcWritableBridge.readDouble(100, __dbResults);
    this.CloudTechnologies_CloudService_One = JdbcWritableBridge.readDouble(101, __dbResults);
    this.CloudTechnologies_CloudService_Two = JdbcWritableBridge.readDouble(102, __dbResults);
    this.CloudTechnologies_CommTech_One = JdbcWritableBridge.readDouble(103, __dbResults);
    this.CloudTechnologies_CommTech_Two = JdbcWritableBridge.readDouble(104, __dbResults);
    this.CloudTechnologies_CRM_One = JdbcWritableBridge.readDouble(105, __dbResults);
    this.CloudTechnologies_CRM_Two = JdbcWritableBridge.readDouble(106, __dbResults);
    this.CloudTechnologies_DataCenterSolutions_One = JdbcWritableBridge.readDouble(107, __dbResults);
    this.CloudTechnologies_DataCenterSolutions_Two = JdbcWritableBridge.readDouble(108, __dbResults);
    this.CloudTechnologies_EnterpriseApplications_One = JdbcWritableBridge.readDouble(109, __dbResults);
    this.CloudTechnologies_EnterpriseApplications_Two = JdbcWritableBridge.readDouble(110, __dbResults);
    this.CloudTechnologies_EnterpriseContent_One = JdbcWritableBridge.readDouble(111, __dbResults);
    this.CloudTechnologies_EnterpriseContent_Two = JdbcWritableBridge.readDouble(112, __dbResults);
    this.CloudTechnologies_HardwareBasic_One = JdbcWritableBridge.readDouble(113, __dbResults);
    this.CloudTechnologies_HardwareBasic_Two = JdbcWritableBridge.readDouble(114, __dbResults);
    this.CloudTechnologies_ITGovernance_One = JdbcWritableBridge.readDouble(115, __dbResults);
    this.CloudTechnologies_ITGovernance_Two = JdbcWritableBridge.readDouble(116, __dbResults);
    this.CloudTechnologies_MarketingPerfMgmt_One = JdbcWritableBridge.readDouble(117, __dbResults);
    this.CloudTechnologies_MarketingPerfMgmt_Two = JdbcWritableBridge.readDouble(118, __dbResults);
    this.CloudTechnologies_NetworkComputing_One = JdbcWritableBridge.readDouble(119, __dbResults);
    this.CloudTechnologies_NetworkComputing_Two = JdbcWritableBridge.readDouble(120, __dbResults);
    this.CloudTechnologies_ProductivitySltns_One = JdbcWritableBridge.readDouble(121, __dbResults);
    this.CloudTechnologies_ProductivitySltns_Two = JdbcWritableBridge.readDouble(122, __dbResults);
    this.CloudTechnologies_ProjectMgnt_One = JdbcWritableBridge.readDouble(123, __dbResults);
    this.CloudTechnologies_ProjectMgnt_Two = JdbcWritableBridge.readDouble(124, __dbResults);
    this.CloudTechnologies_SoftwareBasic_One = JdbcWritableBridge.readDouble(125, __dbResults);
    this.CloudTechnologies_SoftwareBasic_Two = JdbcWritableBridge.readDouble(126, __dbResults);
    this.CloudTechnologies_VerticalMarkets_One = JdbcWritableBridge.readDouble(127, __dbResults);
    this.CloudTechnologies_VerticalMarkets_Two = JdbcWritableBridge.readDouble(128, __dbResults);
    this.CloudTechnologies_WebOrntdArch_One = JdbcWritableBridge.readDouble(129, __dbResults);
    this.CloudTechnologies_WebOrntdArch_Two = JdbcWritableBridge.readDouble(130, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeLong(Nutanix_EventTable_Clean, 1 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeString(LeadID, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(CustomerID, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeLong(PeriodID, 4 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeLong(P1_Target, 5 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeLong(P1_TargetTraining, 6 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeString(P1_Event, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Company, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Email, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Domain, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeLong(AwardYear, 11 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeString(BankruptcyFiled, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(BusinessAnnualSalesAbs, 13 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(BusinessAssets, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(BusinessECommerceSite, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(BusinessEntityType, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(BusinessEstablishedYear, 17 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BusinessEstimatedAnnualSales_k, 18 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BusinessEstimatedEmployees, 19 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BusinessFirmographicsParentEmployees, 20 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BusinessFirmographicsParentRevenue, 21 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(BusinessIndustrySector, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(BusinessRetirementParticipants, 23 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(BusinessSocialPresence, 24 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(BusinessUrlNumPages, 25 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(BusinessType, 26 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(BusinessVCFunded, 27 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DerogatoryIndicator, 28 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ExperianCreditRating, 29 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(FundingAgency, 30 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(FundingAmount, 31 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(FundingAwardAmount, 32 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(FundingFinanceRound, 33 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(FundingFiscalQuarter, 34 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(FundingFiscalYear, 35 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(FundingReceived, 36 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(FundingStage, 37 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(Intelliscore, 38 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(JobsRecentJobs, 39 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(JobsTrendString, 40 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ModelAction, 41 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(PercentileModel, 42 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(RetirementAssetsEOY, 43 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(RetirementAssetsYOY, 44 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(TotalParticipantsSOY, 45 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(UCCFilings, 46 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(UCCFilingsPresent, 47 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Years_in_Business_Code, 48 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Non_Profit_Indicator, 49 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PD_DA_AwardCategory, 50 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PD_DA_JobTitle, 51 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PD_DA_LastSocialActivity_Units, 52 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(PD_DA_MonthsPatentGranted, 53 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(PD_DA_MonthsSinceFundAwardDate, 54 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(PD_DA_PrimarySIC1, 55 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Industry, 56 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(LeadSource, 57 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(AnnualRevenue, 58 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(NumberOfEmployees, 59 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(Alexa_MonthsSinceOnline, 60 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(Alexa_Rank, 61 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(Alexa_ReachPerMillion, 62 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(Alexa_ViewsPerMillion, 63 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(Alexa_ViewsPerUser, 64 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_TechTags_Cnt, 65 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_TotalTech_Cnt, 66 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_ads, 67 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_analytics, 68 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_cdn, 69 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_cdns, 70 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_cms, 71 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_docinfo, 72 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_encoding, 73 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_feeds, 74 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_framework, 75 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_hosting, 76 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_javascript, 77 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_mapping, 78 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_media, 79 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_mx, 80 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_ns, 81 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_parked, 82 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_payment, 83 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_seo_headers, 84 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_seo_meta, 85 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_seo_title, 86 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_Server, 87 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_shop, 88 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_ssl, 89 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_Web_Master, 90 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_Web_Server, 91 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_widgets, 92 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(Activity_ClickLink_cnt, 93 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(Activity_VisitWeb_cnt, 94 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(Activity_InterestingMoment_cnt, 95 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(Activity_OpenEmail_cnt, 96 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(Activity_EmailBncedSft_cnt, 97 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(Activity_FillOutForm_cnt, 98 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(Activity_UnsubscrbEmail_cnt, 99 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(Activity_ClickEmail_cnt, 100 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_CloudService_One, 101 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_CloudService_Two, 102 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_CommTech_One, 103 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_CommTech_Two, 104 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_CRM_One, 105 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_CRM_Two, 106 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_DataCenterSolutions_One, 107 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_DataCenterSolutions_Two, 108 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_EnterpriseApplications_One, 109 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_EnterpriseApplications_Two, 110 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_EnterpriseContent_One, 111 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_EnterpriseContent_Two, 112 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_HardwareBasic_One, 113 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_HardwareBasic_Two, 114 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_ITGovernance_One, 115 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_ITGovernance_Two, 116 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_MarketingPerfMgmt_One, 117 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_MarketingPerfMgmt_Two, 118 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_NetworkComputing_One, 119 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_NetworkComputing_Two, 120 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_ProductivitySltns_One, 121 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_ProductivitySltns_Two, 122 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_ProjectMgnt_One, 123 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_ProjectMgnt_Two, 124 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_SoftwareBasic_One, 125 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_SoftwareBasic_Two, 126 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_VerticalMarkets_One, 127 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_VerticalMarkets_Two, 128 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_WebOrntdArch_One, 129 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_WebOrntdArch_Two, 130 + __off, 6, __dbStmt);
    return 130;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeLong(Nutanix_EventTable_Clean, 1 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeString(LeadID, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(CustomerID, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeLong(PeriodID, 4 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeLong(P1_Target, 5 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeLong(P1_TargetTraining, 6 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeString(P1_Event, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Company, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Email, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Domain, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeLong(AwardYear, 11 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeString(BankruptcyFiled, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(BusinessAnnualSalesAbs, 13 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(BusinessAssets, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(BusinessECommerceSite, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(BusinessEntityType, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(BusinessEstablishedYear, 17 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BusinessEstimatedAnnualSales_k, 18 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BusinessEstimatedEmployees, 19 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BusinessFirmographicsParentEmployees, 20 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BusinessFirmographicsParentRevenue, 21 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(BusinessIndustrySector, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(BusinessRetirementParticipants, 23 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(BusinessSocialPresence, 24 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(BusinessUrlNumPages, 25 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(BusinessType, 26 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(BusinessVCFunded, 27 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DerogatoryIndicator, 28 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ExperianCreditRating, 29 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(FundingAgency, 30 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(FundingAmount, 31 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(FundingAwardAmount, 32 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(FundingFinanceRound, 33 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(FundingFiscalQuarter, 34 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(FundingFiscalYear, 35 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(FundingReceived, 36 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(FundingStage, 37 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(Intelliscore, 38 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(JobsRecentJobs, 39 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(JobsTrendString, 40 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ModelAction, 41 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(PercentileModel, 42 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(RetirementAssetsEOY, 43 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(RetirementAssetsYOY, 44 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(TotalParticipantsSOY, 45 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(UCCFilings, 46 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(UCCFilingsPresent, 47 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Years_in_Business_Code, 48 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Non_Profit_Indicator, 49 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PD_DA_AwardCategory, 50 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PD_DA_JobTitle, 51 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PD_DA_LastSocialActivity_Units, 52 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(PD_DA_MonthsPatentGranted, 53 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(PD_DA_MonthsSinceFundAwardDate, 54 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(PD_DA_PrimarySIC1, 55 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Industry, 56 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(LeadSource, 57 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(AnnualRevenue, 58 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(NumberOfEmployees, 59 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(Alexa_MonthsSinceOnline, 60 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(Alexa_Rank, 61 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(Alexa_ReachPerMillion, 62 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(Alexa_ViewsPerMillion, 63 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(Alexa_ViewsPerUser, 64 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_TechTags_Cnt, 65 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_TotalTech_Cnt, 66 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_ads, 67 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_analytics, 68 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_cdn, 69 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_cdns, 70 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_cms, 71 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_docinfo, 72 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_encoding, 73 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_feeds, 74 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_framework, 75 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_hosting, 76 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_javascript, 77 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_mapping, 78 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_media, 79 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_mx, 80 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_ns, 81 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_parked, 82 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_payment, 83 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_seo_headers, 84 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_seo_meta, 85 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_seo_title, 86 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_Server, 87 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_shop, 88 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_ssl, 89 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_Web_Master, 90 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_Web_Server, 91 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BW_widgets, 92 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(Activity_ClickLink_cnt, 93 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(Activity_VisitWeb_cnt, 94 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(Activity_InterestingMoment_cnt, 95 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(Activity_OpenEmail_cnt, 96 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(Activity_EmailBncedSft_cnt, 97 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(Activity_FillOutForm_cnt, 98 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(Activity_UnsubscrbEmail_cnt, 99 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(Activity_ClickEmail_cnt, 100 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_CloudService_One, 101 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_CloudService_Two, 102 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_CommTech_One, 103 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_CommTech_Two, 104 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_CRM_One, 105 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_CRM_Two, 106 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_DataCenterSolutions_One, 107 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_DataCenterSolutions_Two, 108 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_EnterpriseApplications_One, 109 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_EnterpriseApplications_Two, 110 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_EnterpriseContent_One, 111 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_EnterpriseContent_Two, 112 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_HardwareBasic_One, 113 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_HardwareBasic_Two, 114 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_ITGovernance_One, 115 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_ITGovernance_Two, 116 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_MarketingPerfMgmt_One, 117 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_MarketingPerfMgmt_Two, 118 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_NetworkComputing_One, 119 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_NetworkComputing_Two, 120 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_ProductivitySltns_One, 121 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_ProductivitySltns_Two, 122 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_ProjectMgnt_One, 123 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_ProjectMgnt_Two, 124 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_SoftwareBasic_One, 125 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_SoftwareBasic_Two, 126 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_VerticalMarkets_One, 127 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_VerticalMarkets_Two, 128 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_WebOrntdArch_One, 129 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CloudTechnologies_WebOrntdArch_Two, 130 + __off, 6, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.Nutanix_EventTable_Clean = null;
    } else {
    this.Nutanix_EventTable_Clean = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.LeadID = null;
    } else {
    this.LeadID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CustomerID = null;
    } else {
    this.CustomerID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PeriodID = null;
    } else {
    this.PeriodID = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.P1_Target = null;
    } else {
    this.P1_Target = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.P1_TargetTraining = null;
    } else {
    this.P1_TargetTraining = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.P1_Event = null;
    } else {
    this.P1_Event = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.Company = null;
    } else {
    this.Company = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.Email = null;
    } else {
    this.Email = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.Domain = null;
    } else {
    this.Domain = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.AwardYear = null;
    } else {
    this.AwardYear = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.BankruptcyFiled = null;
    } else {
    this.BankruptcyFiled = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.BusinessAnnualSalesAbs = null;
    } else {
    this.BusinessAnnualSalesAbs = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BusinessAssets = null;
    } else {
    this.BusinessAssets = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.BusinessECommerceSite = null;
    } else {
    this.BusinessECommerceSite = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.BusinessEntityType = null;
    } else {
    this.BusinessEntityType = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.BusinessEstablishedYear = null;
    } else {
    this.BusinessEstablishedYear = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BusinessEstimatedAnnualSales_k = null;
    } else {
    this.BusinessEstimatedAnnualSales_k = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BusinessEstimatedEmployees = null;
    } else {
    this.BusinessEstimatedEmployees = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BusinessFirmographicsParentEmployees = null;
    } else {
    this.BusinessFirmographicsParentEmployees = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BusinessFirmographicsParentRevenue = null;
    } else {
    this.BusinessFirmographicsParentRevenue = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BusinessIndustrySector = null;
    } else {
    this.BusinessIndustrySector = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.BusinessRetirementParticipants = null;
    } else {
    this.BusinessRetirementParticipants = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BusinessSocialPresence = null;
    } else {
    this.BusinessSocialPresence = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.BusinessUrlNumPages = null;
    } else {
    this.BusinessUrlNumPages = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BusinessType = null;
    } else {
    this.BusinessType = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.BusinessVCFunded = null;
    } else {
    this.BusinessVCFunded = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DerogatoryIndicator = null;
    } else {
    this.DerogatoryIndicator = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ExperianCreditRating = null;
    } else {
    this.ExperianCreditRating = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FundingAgency = null;
    } else {
    this.FundingAgency = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FundingAmount = null;
    } else {
    this.FundingAmount = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.FundingAwardAmount = null;
    } else {
    this.FundingAwardAmount = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.FundingFinanceRound = null;
    } else {
    this.FundingFinanceRound = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.FundingFiscalQuarter = null;
    } else {
    this.FundingFiscalQuarter = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.FundingFiscalYear = null;
    } else {
    this.FundingFiscalYear = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.FundingReceived = null;
    } else {
    this.FundingReceived = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.FundingStage = null;
    } else {
    this.FundingStage = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.Intelliscore = null;
    } else {
    this.Intelliscore = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.JobsRecentJobs = null;
    } else {
    this.JobsRecentJobs = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.JobsTrendString = null;
    } else {
    this.JobsTrendString = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ModelAction = null;
    } else {
    this.ModelAction = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PercentileModel = null;
    } else {
    this.PercentileModel = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.RetirementAssetsEOY = null;
    } else {
    this.RetirementAssetsEOY = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.RetirementAssetsYOY = null;
    } else {
    this.RetirementAssetsYOY = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.TotalParticipantsSOY = null;
    } else {
    this.TotalParticipantsSOY = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.UCCFilings = null;
    } else {
    this.UCCFilings = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.UCCFilingsPresent = null;
    } else {
    this.UCCFilingsPresent = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.Years_in_Business_Code = null;
    } else {
    this.Years_in_Business_Code = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.Non_Profit_Indicator = null;
    } else {
    this.Non_Profit_Indicator = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PD_DA_AwardCategory = null;
    } else {
    this.PD_DA_AwardCategory = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PD_DA_JobTitle = null;
    } else {
    this.PD_DA_JobTitle = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PD_DA_LastSocialActivity_Units = null;
    } else {
    this.PD_DA_LastSocialActivity_Units = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PD_DA_MonthsPatentGranted = null;
    } else {
    this.PD_DA_MonthsPatentGranted = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.PD_DA_MonthsSinceFundAwardDate = null;
    } else {
    this.PD_DA_MonthsSinceFundAwardDate = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.PD_DA_PrimarySIC1 = null;
    } else {
    this.PD_DA_PrimarySIC1 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.Industry = null;
    } else {
    this.Industry = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.LeadSource = null;
    } else {
    this.LeadSource = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.AnnualRevenue = null;
    } else {
    this.AnnualRevenue = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.NumberOfEmployees = null;
    } else {
    this.NumberOfEmployees = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.Alexa_MonthsSinceOnline = null;
    } else {
    this.Alexa_MonthsSinceOnline = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.Alexa_Rank = null;
    } else {
    this.Alexa_Rank = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.Alexa_ReachPerMillion = null;
    } else {
    this.Alexa_ReachPerMillion = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.Alexa_ViewsPerMillion = null;
    } else {
    this.Alexa_ViewsPerMillion = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.Alexa_ViewsPerUser = null;
    } else {
    this.Alexa_ViewsPerUser = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BW_TechTags_Cnt = null;
    } else {
    this.BW_TechTags_Cnt = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BW_TotalTech_Cnt = null;
    } else {
    this.BW_TotalTech_Cnt = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BW_ads = null;
    } else {
    this.BW_ads = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BW_analytics = null;
    } else {
    this.BW_analytics = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BW_cdn = null;
    } else {
    this.BW_cdn = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BW_cdns = null;
    } else {
    this.BW_cdns = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BW_cms = null;
    } else {
    this.BW_cms = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BW_docinfo = null;
    } else {
    this.BW_docinfo = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BW_encoding = null;
    } else {
    this.BW_encoding = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BW_feeds = null;
    } else {
    this.BW_feeds = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BW_framework = null;
    } else {
    this.BW_framework = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BW_hosting = null;
    } else {
    this.BW_hosting = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BW_javascript = null;
    } else {
    this.BW_javascript = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BW_mapping = null;
    } else {
    this.BW_mapping = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BW_media = null;
    } else {
    this.BW_media = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BW_mx = null;
    } else {
    this.BW_mx = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BW_ns = null;
    } else {
    this.BW_ns = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BW_parked = null;
    } else {
    this.BW_parked = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BW_payment = null;
    } else {
    this.BW_payment = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BW_seo_headers = null;
    } else {
    this.BW_seo_headers = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BW_seo_meta = null;
    } else {
    this.BW_seo_meta = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BW_seo_title = null;
    } else {
    this.BW_seo_title = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BW_Server = null;
    } else {
    this.BW_Server = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BW_shop = null;
    } else {
    this.BW_shop = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BW_ssl = null;
    } else {
    this.BW_ssl = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BW_Web_Master = null;
    } else {
    this.BW_Web_Master = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BW_Web_Server = null;
    } else {
    this.BW_Web_Server = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BW_widgets = null;
    } else {
    this.BW_widgets = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.Activity_ClickLink_cnt = null;
    } else {
    this.Activity_ClickLink_cnt = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.Activity_VisitWeb_cnt = null;
    } else {
    this.Activity_VisitWeb_cnt = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.Activity_InterestingMoment_cnt = null;
    } else {
    this.Activity_InterestingMoment_cnt = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.Activity_OpenEmail_cnt = null;
    } else {
    this.Activity_OpenEmail_cnt = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.Activity_EmailBncedSft_cnt = null;
    } else {
    this.Activity_EmailBncedSft_cnt = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.Activity_FillOutForm_cnt = null;
    } else {
    this.Activity_FillOutForm_cnt = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.Activity_UnsubscrbEmail_cnt = null;
    } else {
    this.Activity_UnsubscrbEmail_cnt = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.Activity_ClickEmail_cnt = null;
    } else {
    this.Activity_ClickEmail_cnt = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_CloudService_One = null;
    } else {
    this.CloudTechnologies_CloudService_One = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_CloudService_Two = null;
    } else {
    this.CloudTechnologies_CloudService_Two = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_CommTech_One = null;
    } else {
    this.CloudTechnologies_CommTech_One = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_CommTech_Two = null;
    } else {
    this.CloudTechnologies_CommTech_Two = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_CRM_One = null;
    } else {
    this.CloudTechnologies_CRM_One = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_CRM_Two = null;
    } else {
    this.CloudTechnologies_CRM_Two = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_DataCenterSolutions_One = null;
    } else {
    this.CloudTechnologies_DataCenterSolutions_One = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_DataCenterSolutions_Two = null;
    } else {
    this.CloudTechnologies_DataCenterSolutions_Two = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_EnterpriseApplications_One = null;
    } else {
    this.CloudTechnologies_EnterpriseApplications_One = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_EnterpriseApplications_Two = null;
    } else {
    this.CloudTechnologies_EnterpriseApplications_Two = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_EnterpriseContent_One = null;
    } else {
    this.CloudTechnologies_EnterpriseContent_One = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_EnterpriseContent_Two = null;
    } else {
    this.CloudTechnologies_EnterpriseContent_Two = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_HardwareBasic_One = null;
    } else {
    this.CloudTechnologies_HardwareBasic_One = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_HardwareBasic_Two = null;
    } else {
    this.CloudTechnologies_HardwareBasic_Two = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_ITGovernance_One = null;
    } else {
    this.CloudTechnologies_ITGovernance_One = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_ITGovernance_Two = null;
    } else {
    this.CloudTechnologies_ITGovernance_Two = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_MarketingPerfMgmt_One = null;
    } else {
    this.CloudTechnologies_MarketingPerfMgmt_One = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_MarketingPerfMgmt_Two = null;
    } else {
    this.CloudTechnologies_MarketingPerfMgmt_Two = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_NetworkComputing_One = null;
    } else {
    this.CloudTechnologies_NetworkComputing_One = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_NetworkComputing_Two = null;
    } else {
    this.CloudTechnologies_NetworkComputing_Two = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_ProductivitySltns_One = null;
    } else {
    this.CloudTechnologies_ProductivitySltns_One = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_ProductivitySltns_Two = null;
    } else {
    this.CloudTechnologies_ProductivitySltns_Two = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_ProjectMgnt_One = null;
    } else {
    this.CloudTechnologies_ProjectMgnt_One = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_ProjectMgnt_Two = null;
    } else {
    this.CloudTechnologies_ProjectMgnt_Two = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_SoftwareBasic_One = null;
    } else {
    this.CloudTechnologies_SoftwareBasic_One = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_SoftwareBasic_Two = null;
    } else {
    this.CloudTechnologies_SoftwareBasic_Two = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_VerticalMarkets_One = null;
    } else {
    this.CloudTechnologies_VerticalMarkets_One = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_VerticalMarkets_Two = null;
    } else {
    this.CloudTechnologies_VerticalMarkets_Two = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_WebOrntdArch_One = null;
    } else {
    this.CloudTechnologies_WebOrntdArch_One = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CloudTechnologies_WebOrntdArch_Two = null;
    } else {
    this.CloudTechnologies_WebOrntdArch_Two = Double.valueOf(__dataIn.readDouble());
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.Nutanix_EventTable_Clean) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.Nutanix_EventTable_Clean);
    }
    if (null == this.LeadID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, LeadID);
    }
    if (null == this.CustomerID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CustomerID);
    }
    if (null == this.PeriodID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.PeriodID);
    }
    if (null == this.P1_Target) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.P1_Target);
    }
    if (null == this.P1_TargetTraining) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.P1_TargetTraining);
    }
    if (null == this.P1_Event) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, P1_Event);
    }
    if (null == this.Company) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Company);
    }
    if (null == this.Email) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Email);
    }
    if (null == this.Domain) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Domain);
    }
    if (null == this.AwardYear) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.AwardYear);
    }
    if (null == this.BankruptcyFiled) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BankruptcyFiled);
    }
    if (null == this.BusinessAnnualSalesAbs) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BusinessAnnualSalesAbs);
    }
    if (null == this.BusinessAssets) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BusinessAssets);
    }
    if (null == this.BusinessECommerceSite) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BusinessECommerceSite);
    }
    if (null == this.BusinessEntityType) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BusinessEntityType);
    }
    if (null == this.BusinessEstablishedYear) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BusinessEstablishedYear);
    }
    if (null == this.BusinessEstimatedAnnualSales_k) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BusinessEstimatedAnnualSales_k);
    }
    if (null == this.BusinessEstimatedEmployees) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BusinessEstimatedEmployees);
    }
    if (null == this.BusinessFirmographicsParentEmployees) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BusinessFirmographicsParentEmployees);
    }
    if (null == this.BusinessFirmographicsParentRevenue) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BusinessFirmographicsParentRevenue);
    }
    if (null == this.BusinessIndustrySector) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BusinessIndustrySector);
    }
    if (null == this.BusinessRetirementParticipants) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BusinessRetirementParticipants);
    }
    if (null == this.BusinessSocialPresence) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BusinessSocialPresence);
    }
    if (null == this.BusinessUrlNumPages) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BusinessUrlNumPages);
    }
    if (null == this.BusinessType) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BusinessType);
    }
    if (null == this.BusinessVCFunded) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BusinessVCFunded);
    }
    if (null == this.DerogatoryIndicator) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DerogatoryIndicator);
    }
    if (null == this.ExperianCreditRating) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ExperianCreditRating);
    }
    if (null == this.FundingAgency) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FundingAgency);
    }
    if (null == this.FundingAmount) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.FundingAmount);
    }
    if (null == this.FundingAwardAmount) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.FundingAwardAmount);
    }
    if (null == this.FundingFinanceRound) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.FundingFinanceRound);
    }
    if (null == this.FundingFiscalQuarter) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.FundingFiscalQuarter);
    }
    if (null == this.FundingFiscalYear) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.FundingFiscalYear);
    }
    if (null == this.FundingReceived) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.FundingReceived);
    }
    if (null == this.FundingStage) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FundingStage);
    }
    if (null == this.Intelliscore) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.Intelliscore);
    }
    if (null == this.JobsRecentJobs) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.JobsRecentJobs);
    }
    if (null == this.JobsTrendString) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, JobsTrendString);
    }
    if (null == this.ModelAction) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ModelAction);
    }
    if (null == this.PercentileModel) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.PercentileModel);
    }
    if (null == this.RetirementAssetsEOY) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.RetirementAssetsEOY);
    }
    if (null == this.RetirementAssetsYOY) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.RetirementAssetsYOY);
    }
    if (null == this.TotalParticipantsSOY) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.TotalParticipantsSOY);
    }
    if (null == this.UCCFilings) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.UCCFilings);
    }
    if (null == this.UCCFilingsPresent) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, UCCFilingsPresent);
    }
    if (null == this.Years_in_Business_Code) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Years_in_Business_Code);
    }
    if (null == this.Non_Profit_Indicator) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Non_Profit_Indicator);
    }
    if (null == this.PD_DA_AwardCategory) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PD_DA_AwardCategory);
    }
    if (null == this.PD_DA_JobTitle) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PD_DA_JobTitle);
    }
    if (null == this.PD_DA_LastSocialActivity_Units) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PD_DA_LastSocialActivity_Units);
    }
    if (null == this.PD_DA_MonthsPatentGranted) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.PD_DA_MonthsPatentGranted);
    }
    if (null == this.PD_DA_MonthsSinceFundAwardDate) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.PD_DA_MonthsSinceFundAwardDate);
    }
    if (null == this.PD_DA_PrimarySIC1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PD_DA_PrimarySIC1);
    }
    if (null == this.Industry) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Industry);
    }
    if (null == this.LeadSource) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, LeadSource);
    }
    if (null == this.AnnualRevenue) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.AnnualRevenue);
    }
    if (null == this.NumberOfEmployees) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.NumberOfEmployees);
    }
    if (null == this.Alexa_MonthsSinceOnline) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.Alexa_MonthsSinceOnline);
    }
    if (null == this.Alexa_Rank) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.Alexa_Rank);
    }
    if (null == this.Alexa_ReachPerMillion) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.Alexa_ReachPerMillion);
    }
    if (null == this.Alexa_ViewsPerMillion) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.Alexa_ViewsPerMillion);
    }
    if (null == this.Alexa_ViewsPerUser) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.Alexa_ViewsPerUser);
    }
    if (null == this.BW_TechTags_Cnt) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_TechTags_Cnt);
    }
    if (null == this.BW_TotalTech_Cnt) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_TotalTech_Cnt);
    }
    if (null == this.BW_ads) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_ads);
    }
    if (null == this.BW_analytics) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_analytics);
    }
    if (null == this.BW_cdn) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_cdn);
    }
    if (null == this.BW_cdns) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_cdns);
    }
    if (null == this.BW_cms) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_cms);
    }
    if (null == this.BW_docinfo) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_docinfo);
    }
    if (null == this.BW_encoding) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_encoding);
    }
    if (null == this.BW_feeds) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_feeds);
    }
    if (null == this.BW_framework) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_framework);
    }
    if (null == this.BW_hosting) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_hosting);
    }
    if (null == this.BW_javascript) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_javascript);
    }
    if (null == this.BW_mapping) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_mapping);
    }
    if (null == this.BW_media) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_media);
    }
    if (null == this.BW_mx) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_mx);
    }
    if (null == this.BW_ns) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_ns);
    }
    if (null == this.BW_parked) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_parked);
    }
    if (null == this.BW_payment) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_payment);
    }
    if (null == this.BW_seo_headers) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_seo_headers);
    }
    if (null == this.BW_seo_meta) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_seo_meta);
    }
    if (null == this.BW_seo_title) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_seo_title);
    }
    if (null == this.BW_Server) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_Server);
    }
    if (null == this.BW_shop) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_shop);
    }
    if (null == this.BW_ssl) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_ssl);
    }
    if (null == this.BW_Web_Master) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_Web_Master);
    }
    if (null == this.BW_Web_Server) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_Web_Server);
    }
    if (null == this.BW_widgets) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_widgets);
    }
    if (null == this.Activity_ClickLink_cnt) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.Activity_ClickLink_cnt);
    }
    if (null == this.Activity_VisitWeb_cnt) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.Activity_VisitWeb_cnt);
    }
    if (null == this.Activity_InterestingMoment_cnt) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.Activity_InterestingMoment_cnt);
    }
    if (null == this.Activity_OpenEmail_cnt) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.Activity_OpenEmail_cnt);
    }
    if (null == this.Activity_EmailBncedSft_cnt) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.Activity_EmailBncedSft_cnt);
    }
    if (null == this.Activity_FillOutForm_cnt) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.Activity_FillOutForm_cnt);
    }
    if (null == this.Activity_UnsubscrbEmail_cnt) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.Activity_UnsubscrbEmail_cnt);
    }
    if (null == this.Activity_ClickEmail_cnt) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.Activity_ClickEmail_cnt);
    }
    if (null == this.CloudTechnologies_CloudService_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_CloudService_One);
    }
    if (null == this.CloudTechnologies_CloudService_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_CloudService_Two);
    }
    if (null == this.CloudTechnologies_CommTech_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_CommTech_One);
    }
    if (null == this.CloudTechnologies_CommTech_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_CommTech_Two);
    }
    if (null == this.CloudTechnologies_CRM_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_CRM_One);
    }
    if (null == this.CloudTechnologies_CRM_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_CRM_Two);
    }
    if (null == this.CloudTechnologies_DataCenterSolutions_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_DataCenterSolutions_One);
    }
    if (null == this.CloudTechnologies_DataCenterSolutions_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_DataCenterSolutions_Two);
    }
    if (null == this.CloudTechnologies_EnterpriseApplications_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_EnterpriseApplications_One);
    }
    if (null == this.CloudTechnologies_EnterpriseApplications_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_EnterpriseApplications_Two);
    }
    if (null == this.CloudTechnologies_EnterpriseContent_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_EnterpriseContent_One);
    }
    if (null == this.CloudTechnologies_EnterpriseContent_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_EnterpriseContent_Two);
    }
    if (null == this.CloudTechnologies_HardwareBasic_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_HardwareBasic_One);
    }
    if (null == this.CloudTechnologies_HardwareBasic_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_HardwareBasic_Two);
    }
    if (null == this.CloudTechnologies_ITGovernance_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_ITGovernance_One);
    }
    if (null == this.CloudTechnologies_ITGovernance_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_ITGovernance_Two);
    }
    if (null == this.CloudTechnologies_MarketingPerfMgmt_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_MarketingPerfMgmt_One);
    }
    if (null == this.CloudTechnologies_MarketingPerfMgmt_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_MarketingPerfMgmt_Two);
    }
    if (null == this.CloudTechnologies_NetworkComputing_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_NetworkComputing_One);
    }
    if (null == this.CloudTechnologies_NetworkComputing_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_NetworkComputing_Two);
    }
    if (null == this.CloudTechnologies_ProductivitySltns_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_ProductivitySltns_One);
    }
    if (null == this.CloudTechnologies_ProductivitySltns_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_ProductivitySltns_Two);
    }
    if (null == this.CloudTechnologies_ProjectMgnt_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_ProjectMgnt_One);
    }
    if (null == this.CloudTechnologies_ProjectMgnt_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_ProjectMgnt_Two);
    }
    if (null == this.CloudTechnologies_SoftwareBasic_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_SoftwareBasic_One);
    }
    if (null == this.CloudTechnologies_SoftwareBasic_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_SoftwareBasic_Two);
    }
    if (null == this.CloudTechnologies_VerticalMarkets_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_VerticalMarkets_One);
    }
    if (null == this.CloudTechnologies_VerticalMarkets_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_VerticalMarkets_Two);
    }
    if (null == this.CloudTechnologies_WebOrntdArch_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_WebOrntdArch_One);
    }
    if (null == this.CloudTechnologies_WebOrntdArch_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_WebOrntdArch_Two);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.Nutanix_EventTable_Clean) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.Nutanix_EventTable_Clean);
    }
    if (null == this.LeadID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, LeadID);
    }
    if (null == this.CustomerID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CustomerID);
    }
    if (null == this.PeriodID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.PeriodID);
    }
    if (null == this.P1_Target) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.P1_Target);
    }
    if (null == this.P1_TargetTraining) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.P1_TargetTraining);
    }
    if (null == this.P1_Event) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, P1_Event);
    }
    if (null == this.Company) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Company);
    }
    if (null == this.Email) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Email);
    }
    if (null == this.Domain) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Domain);
    }
    if (null == this.AwardYear) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.AwardYear);
    }
    if (null == this.BankruptcyFiled) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BankruptcyFiled);
    }
    if (null == this.BusinessAnnualSalesAbs) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BusinessAnnualSalesAbs);
    }
    if (null == this.BusinessAssets) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BusinessAssets);
    }
    if (null == this.BusinessECommerceSite) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BusinessECommerceSite);
    }
    if (null == this.BusinessEntityType) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BusinessEntityType);
    }
    if (null == this.BusinessEstablishedYear) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BusinessEstablishedYear);
    }
    if (null == this.BusinessEstimatedAnnualSales_k) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BusinessEstimatedAnnualSales_k);
    }
    if (null == this.BusinessEstimatedEmployees) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BusinessEstimatedEmployees);
    }
    if (null == this.BusinessFirmographicsParentEmployees) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BusinessFirmographicsParentEmployees);
    }
    if (null == this.BusinessFirmographicsParentRevenue) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BusinessFirmographicsParentRevenue);
    }
    if (null == this.BusinessIndustrySector) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BusinessIndustrySector);
    }
    if (null == this.BusinessRetirementParticipants) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BusinessRetirementParticipants);
    }
    if (null == this.BusinessSocialPresence) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BusinessSocialPresence);
    }
    if (null == this.BusinessUrlNumPages) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BusinessUrlNumPages);
    }
    if (null == this.BusinessType) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BusinessType);
    }
    if (null == this.BusinessVCFunded) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BusinessVCFunded);
    }
    if (null == this.DerogatoryIndicator) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DerogatoryIndicator);
    }
    if (null == this.ExperianCreditRating) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ExperianCreditRating);
    }
    if (null == this.FundingAgency) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FundingAgency);
    }
    if (null == this.FundingAmount) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.FundingAmount);
    }
    if (null == this.FundingAwardAmount) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.FundingAwardAmount);
    }
    if (null == this.FundingFinanceRound) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.FundingFinanceRound);
    }
    if (null == this.FundingFiscalQuarter) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.FundingFiscalQuarter);
    }
    if (null == this.FundingFiscalYear) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.FundingFiscalYear);
    }
    if (null == this.FundingReceived) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.FundingReceived);
    }
    if (null == this.FundingStage) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FundingStage);
    }
    if (null == this.Intelliscore) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.Intelliscore);
    }
    if (null == this.JobsRecentJobs) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.JobsRecentJobs);
    }
    if (null == this.JobsTrendString) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, JobsTrendString);
    }
    if (null == this.ModelAction) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ModelAction);
    }
    if (null == this.PercentileModel) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.PercentileModel);
    }
    if (null == this.RetirementAssetsEOY) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.RetirementAssetsEOY);
    }
    if (null == this.RetirementAssetsYOY) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.RetirementAssetsYOY);
    }
    if (null == this.TotalParticipantsSOY) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.TotalParticipantsSOY);
    }
    if (null == this.UCCFilings) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.UCCFilings);
    }
    if (null == this.UCCFilingsPresent) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, UCCFilingsPresent);
    }
    if (null == this.Years_in_Business_Code) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Years_in_Business_Code);
    }
    if (null == this.Non_Profit_Indicator) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Non_Profit_Indicator);
    }
    if (null == this.PD_DA_AwardCategory) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PD_DA_AwardCategory);
    }
    if (null == this.PD_DA_JobTitle) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PD_DA_JobTitle);
    }
    if (null == this.PD_DA_LastSocialActivity_Units) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PD_DA_LastSocialActivity_Units);
    }
    if (null == this.PD_DA_MonthsPatentGranted) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.PD_DA_MonthsPatentGranted);
    }
    if (null == this.PD_DA_MonthsSinceFundAwardDate) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.PD_DA_MonthsSinceFundAwardDate);
    }
    if (null == this.PD_DA_PrimarySIC1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PD_DA_PrimarySIC1);
    }
    if (null == this.Industry) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Industry);
    }
    if (null == this.LeadSource) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, LeadSource);
    }
    if (null == this.AnnualRevenue) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.AnnualRevenue);
    }
    if (null == this.NumberOfEmployees) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.NumberOfEmployees);
    }
    if (null == this.Alexa_MonthsSinceOnline) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.Alexa_MonthsSinceOnline);
    }
    if (null == this.Alexa_Rank) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.Alexa_Rank);
    }
    if (null == this.Alexa_ReachPerMillion) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.Alexa_ReachPerMillion);
    }
    if (null == this.Alexa_ViewsPerMillion) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.Alexa_ViewsPerMillion);
    }
    if (null == this.Alexa_ViewsPerUser) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.Alexa_ViewsPerUser);
    }
    if (null == this.BW_TechTags_Cnt) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_TechTags_Cnt);
    }
    if (null == this.BW_TotalTech_Cnt) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_TotalTech_Cnt);
    }
    if (null == this.BW_ads) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_ads);
    }
    if (null == this.BW_analytics) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_analytics);
    }
    if (null == this.BW_cdn) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_cdn);
    }
    if (null == this.BW_cdns) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_cdns);
    }
    if (null == this.BW_cms) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_cms);
    }
    if (null == this.BW_docinfo) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_docinfo);
    }
    if (null == this.BW_encoding) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_encoding);
    }
    if (null == this.BW_feeds) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_feeds);
    }
    if (null == this.BW_framework) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_framework);
    }
    if (null == this.BW_hosting) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_hosting);
    }
    if (null == this.BW_javascript) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_javascript);
    }
    if (null == this.BW_mapping) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_mapping);
    }
    if (null == this.BW_media) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_media);
    }
    if (null == this.BW_mx) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_mx);
    }
    if (null == this.BW_ns) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_ns);
    }
    if (null == this.BW_parked) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_parked);
    }
    if (null == this.BW_payment) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_payment);
    }
    if (null == this.BW_seo_headers) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_seo_headers);
    }
    if (null == this.BW_seo_meta) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_seo_meta);
    }
    if (null == this.BW_seo_title) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_seo_title);
    }
    if (null == this.BW_Server) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_Server);
    }
    if (null == this.BW_shop) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_shop);
    }
    if (null == this.BW_ssl) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_ssl);
    }
    if (null == this.BW_Web_Master) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_Web_Master);
    }
    if (null == this.BW_Web_Server) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_Web_Server);
    }
    if (null == this.BW_widgets) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BW_widgets);
    }
    if (null == this.Activity_ClickLink_cnt) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.Activity_ClickLink_cnt);
    }
    if (null == this.Activity_VisitWeb_cnt) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.Activity_VisitWeb_cnt);
    }
    if (null == this.Activity_InterestingMoment_cnt) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.Activity_InterestingMoment_cnt);
    }
    if (null == this.Activity_OpenEmail_cnt) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.Activity_OpenEmail_cnt);
    }
    if (null == this.Activity_EmailBncedSft_cnt) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.Activity_EmailBncedSft_cnt);
    }
    if (null == this.Activity_FillOutForm_cnt) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.Activity_FillOutForm_cnt);
    }
    if (null == this.Activity_UnsubscrbEmail_cnt) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.Activity_UnsubscrbEmail_cnt);
    }
    if (null == this.Activity_ClickEmail_cnt) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.Activity_ClickEmail_cnt);
    }
    if (null == this.CloudTechnologies_CloudService_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_CloudService_One);
    }
    if (null == this.CloudTechnologies_CloudService_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_CloudService_Two);
    }
    if (null == this.CloudTechnologies_CommTech_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_CommTech_One);
    }
    if (null == this.CloudTechnologies_CommTech_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_CommTech_Two);
    }
    if (null == this.CloudTechnologies_CRM_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_CRM_One);
    }
    if (null == this.CloudTechnologies_CRM_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_CRM_Two);
    }
    if (null == this.CloudTechnologies_DataCenterSolutions_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_DataCenterSolutions_One);
    }
    if (null == this.CloudTechnologies_DataCenterSolutions_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_DataCenterSolutions_Two);
    }
    if (null == this.CloudTechnologies_EnterpriseApplications_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_EnterpriseApplications_One);
    }
    if (null == this.CloudTechnologies_EnterpriseApplications_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_EnterpriseApplications_Two);
    }
    if (null == this.CloudTechnologies_EnterpriseContent_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_EnterpriseContent_One);
    }
    if (null == this.CloudTechnologies_EnterpriseContent_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_EnterpriseContent_Two);
    }
    if (null == this.CloudTechnologies_HardwareBasic_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_HardwareBasic_One);
    }
    if (null == this.CloudTechnologies_HardwareBasic_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_HardwareBasic_Two);
    }
    if (null == this.CloudTechnologies_ITGovernance_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_ITGovernance_One);
    }
    if (null == this.CloudTechnologies_ITGovernance_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_ITGovernance_Two);
    }
    if (null == this.CloudTechnologies_MarketingPerfMgmt_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_MarketingPerfMgmt_One);
    }
    if (null == this.CloudTechnologies_MarketingPerfMgmt_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_MarketingPerfMgmt_Two);
    }
    if (null == this.CloudTechnologies_NetworkComputing_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_NetworkComputing_One);
    }
    if (null == this.CloudTechnologies_NetworkComputing_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_NetworkComputing_Two);
    }
    if (null == this.CloudTechnologies_ProductivitySltns_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_ProductivitySltns_One);
    }
    if (null == this.CloudTechnologies_ProductivitySltns_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_ProductivitySltns_Two);
    }
    if (null == this.CloudTechnologies_ProjectMgnt_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_ProjectMgnt_One);
    }
    if (null == this.CloudTechnologies_ProjectMgnt_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_ProjectMgnt_Two);
    }
    if (null == this.CloudTechnologies_SoftwareBasic_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_SoftwareBasic_One);
    }
    if (null == this.CloudTechnologies_SoftwareBasic_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_SoftwareBasic_Two);
    }
    if (null == this.CloudTechnologies_VerticalMarkets_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_VerticalMarkets_One);
    }
    if (null == this.CloudTechnologies_VerticalMarkets_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_VerticalMarkets_Two);
    }
    if (null == this.CloudTechnologies_WebOrntdArch_One) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_WebOrntdArch_One);
    }
    if (null == this.CloudTechnologies_WebOrntdArch_Two) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CloudTechnologies_WebOrntdArch_Two);
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(Nutanix_EventTable_Clean==null?"null":"" + Nutanix_EventTable_Clean, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(LeadID==null?"null":LeadID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CustomerID==null?"null":CustomerID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PeriodID==null?"null":"" + PeriodID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(P1_Target==null?"null":"" + P1_Target, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(P1_TargetTraining==null?"null":"" + P1_TargetTraining, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(P1_Event==null?"null":P1_Event, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Company==null?"null":Company, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Email==null?"null":Email, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Domain==null?"null":Domain, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(AwardYear==null?"null":"" + AwardYear, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BankruptcyFiled==null?"null":BankruptcyFiled, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessAnnualSalesAbs==null?"null":"" + BusinessAnnualSalesAbs, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessAssets==null?"null":BusinessAssets, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessECommerceSite==null?"null":BusinessECommerceSite, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessEntityType==null?"null":BusinessEntityType, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessEstablishedYear==null?"null":"" + BusinessEstablishedYear, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessEstimatedAnnualSales_k==null?"null":"" + BusinessEstimatedAnnualSales_k, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessEstimatedEmployees==null?"null":"" + BusinessEstimatedEmployees, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessFirmographicsParentEmployees==null?"null":"" + BusinessFirmographicsParentEmployees, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessFirmographicsParentRevenue==null?"null":"" + BusinessFirmographicsParentRevenue, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessIndustrySector==null?"null":BusinessIndustrySector, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessRetirementParticipants==null?"null":"" + BusinessRetirementParticipants, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessSocialPresence==null?"null":BusinessSocialPresence, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessUrlNumPages==null?"null":"" + BusinessUrlNumPages, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessType==null?"null":BusinessType, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessVCFunded==null?"null":BusinessVCFunded, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DerogatoryIndicator==null?"null":DerogatoryIndicator, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ExperianCreditRating==null?"null":ExperianCreditRating, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FundingAgency==null?"null":FundingAgency, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FundingAmount==null?"null":"" + FundingAmount, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FundingAwardAmount==null?"null":"" + FundingAwardAmount, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FundingFinanceRound==null?"null":"" + FundingFinanceRound, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FundingFiscalQuarter==null?"null":"" + FundingFiscalQuarter, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FundingFiscalYear==null?"null":"" + FundingFiscalYear, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FundingReceived==null?"null":"" + FundingReceived, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FundingStage==null?"null":FundingStage, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Intelliscore==null?"null":"" + Intelliscore, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(JobsRecentJobs==null?"null":"" + JobsRecentJobs, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(JobsTrendString==null?"null":JobsTrendString, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ModelAction==null?"null":ModelAction, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PercentileModel==null?"null":"" + PercentileModel, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RetirementAssetsEOY==null?"null":"" + RetirementAssetsEOY, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RetirementAssetsYOY==null?"null":"" + RetirementAssetsYOY, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TotalParticipantsSOY==null?"null":"" + TotalParticipantsSOY, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(UCCFilings==null?"null":"" + UCCFilings, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(UCCFilingsPresent==null?"null":UCCFilingsPresent, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Years_in_Business_Code==null?"null":Years_in_Business_Code, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Non_Profit_Indicator==null?"null":Non_Profit_Indicator, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PD_DA_AwardCategory==null?"null":PD_DA_AwardCategory, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PD_DA_JobTitle==null?"null":PD_DA_JobTitle, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PD_DA_LastSocialActivity_Units==null?"null":PD_DA_LastSocialActivity_Units, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PD_DA_MonthsPatentGranted==null?"null":"" + PD_DA_MonthsPatentGranted, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PD_DA_MonthsSinceFundAwardDate==null?"null":"" + PD_DA_MonthsSinceFundAwardDate, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PD_DA_PrimarySIC1==null?"null":PD_DA_PrimarySIC1, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Industry==null?"null":Industry, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(LeadSource==null?"null":LeadSource, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(AnnualRevenue==null?"null":"" + AnnualRevenue, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(NumberOfEmployees==null?"null":"" + NumberOfEmployees, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Alexa_MonthsSinceOnline==null?"null":"" + Alexa_MonthsSinceOnline, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Alexa_Rank==null?"null":"" + Alexa_Rank, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Alexa_ReachPerMillion==null?"null":"" + Alexa_ReachPerMillion, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Alexa_ViewsPerMillion==null?"null":"" + Alexa_ViewsPerMillion, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Alexa_ViewsPerUser==null?"null":"" + Alexa_ViewsPerUser, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_TechTags_Cnt==null?"null":"" + BW_TechTags_Cnt, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_TotalTech_Cnt==null?"null":"" + BW_TotalTech_Cnt, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_ads==null?"null":"" + BW_ads, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_analytics==null?"null":"" + BW_analytics, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_cdn==null?"null":"" + BW_cdn, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_cdns==null?"null":"" + BW_cdns, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_cms==null?"null":"" + BW_cms, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_docinfo==null?"null":"" + BW_docinfo, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_encoding==null?"null":"" + BW_encoding, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_feeds==null?"null":"" + BW_feeds, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_framework==null?"null":"" + BW_framework, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_hosting==null?"null":"" + BW_hosting, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_javascript==null?"null":"" + BW_javascript, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_mapping==null?"null":"" + BW_mapping, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_media==null?"null":"" + BW_media, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_mx==null?"null":"" + BW_mx, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_ns==null?"null":"" + BW_ns, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_parked==null?"null":"" + BW_parked, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_payment==null?"null":"" + BW_payment, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_seo_headers==null?"null":"" + BW_seo_headers, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_seo_meta==null?"null":"" + BW_seo_meta, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_seo_title==null?"null":"" + BW_seo_title, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_Server==null?"null":"" + BW_Server, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_shop==null?"null":"" + BW_shop, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_ssl==null?"null":"" + BW_ssl, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_Web_Master==null?"null":"" + BW_Web_Master, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_Web_Server==null?"null":"" + BW_Web_Server, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_widgets==null?"null":"" + BW_widgets, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Activity_ClickLink_cnt==null?"null":"" + Activity_ClickLink_cnt, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Activity_VisitWeb_cnt==null?"null":"" + Activity_VisitWeb_cnt, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Activity_InterestingMoment_cnt==null?"null":"" + Activity_InterestingMoment_cnt, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Activity_OpenEmail_cnt==null?"null":"" + Activity_OpenEmail_cnt, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Activity_EmailBncedSft_cnt==null?"null":"" + Activity_EmailBncedSft_cnt, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Activity_FillOutForm_cnt==null?"null":"" + Activity_FillOutForm_cnt, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Activity_UnsubscrbEmail_cnt==null?"null":"" + Activity_UnsubscrbEmail_cnt, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Activity_ClickEmail_cnt==null?"null":"" + Activity_ClickEmail_cnt, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_CloudService_One==null?"null":"" + CloudTechnologies_CloudService_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_CloudService_Two==null?"null":"" + CloudTechnologies_CloudService_Two, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_CommTech_One==null?"null":"" + CloudTechnologies_CommTech_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_CommTech_Two==null?"null":"" + CloudTechnologies_CommTech_Two, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_CRM_One==null?"null":"" + CloudTechnologies_CRM_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_CRM_Two==null?"null":"" + CloudTechnologies_CRM_Two, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_DataCenterSolutions_One==null?"null":"" + CloudTechnologies_DataCenterSolutions_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_DataCenterSolutions_Two==null?"null":"" + CloudTechnologies_DataCenterSolutions_Two, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_EnterpriseApplications_One==null?"null":"" + CloudTechnologies_EnterpriseApplications_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_EnterpriseApplications_Two==null?"null":"" + CloudTechnologies_EnterpriseApplications_Two, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_EnterpriseContent_One==null?"null":"" + CloudTechnologies_EnterpriseContent_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_EnterpriseContent_Two==null?"null":"" + CloudTechnologies_EnterpriseContent_Two, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_HardwareBasic_One==null?"null":"" + CloudTechnologies_HardwareBasic_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_HardwareBasic_Two==null?"null":"" + CloudTechnologies_HardwareBasic_Two, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_ITGovernance_One==null?"null":"" + CloudTechnologies_ITGovernance_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_ITGovernance_Two==null?"null":"" + CloudTechnologies_ITGovernance_Two, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_MarketingPerfMgmt_One==null?"null":"" + CloudTechnologies_MarketingPerfMgmt_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_MarketingPerfMgmt_Two==null?"null":"" + CloudTechnologies_MarketingPerfMgmt_Two, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_NetworkComputing_One==null?"null":"" + CloudTechnologies_NetworkComputing_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_NetworkComputing_Two==null?"null":"" + CloudTechnologies_NetworkComputing_Two, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_ProductivitySltns_One==null?"null":"" + CloudTechnologies_ProductivitySltns_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_ProductivitySltns_Two==null?"null":"" + CloudTechnologies_ProductivitySltns_Two, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_ProjectMgnt_One==null?"null":"" + CloudTechnologies_ProjectMgnt_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_ProjectMgnt_Two==null?"null":"" + CloudTechnologies_ProjectMgnt_Two, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_SoftwareBasic_One==null?"null":"" + CloudTechnologies_SoftwareBasic_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_SoftwareBasic_Two==null?"null":"" + CloudTechnologies_SoftwareBasic_Two, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_VerticalMarkets_One==null?"null":"" + CloudTechnologies_VerticalMarkets_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_VerticalMarkets_Two==null?"null":"" + CloudTechnologies_VerticalMarkets_Two, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_WebOrntdArch_One==null?"null":"" + CloudTechnologies_WebOrntdArch_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_WebOrntdArch_Two==null?"null":"" + CloudTechnologies_WebOrntdArch_Two, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(Nutanix_EventTable_Clean==null?"null":"" + Nutanix_EventTable_Clean, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(LeadID==null?"null":LeadID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CustomerID==null?"null":CustomerID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PeriodID==null?"null":"" + PeriodID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(P1_Target==null?"null":"" + P1_Target, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(P1_TargetTraining==null?"null":"" + P1_TargetTraining, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(P1_Event==null?"null":P1_Event, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Company==null?"null":Company, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Email==null?"null":Email, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Domain==null?"null":Domain, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(AwardYear==null?"null":"" + AwardYear, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BankruptcyFiled==null?"null":BankruptcyFiled, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessAnnualSalesAbs==null?"null":"" + BusinessAnnualSalesAbs, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessAssets==null?"null":BusinessAssets, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessECommerceSite==null?"null":BusinessECommerceSite, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessEntityType==null?"null":BusinessEntityType, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessEstablishedYear==null?"null":"" + BusinessEstablishedYear, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessEstimatedAnnualSales_k==null?"null":"" + BusinessEstimatedAnnualSales_k, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessEstimatedEmployees==null?"null":"" + BusinessEstimatedEmployees, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessFirmographicsParentEmployees==null?"null":"" + BusinessFirmographicsParentEmployees, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessFirmographicsParentRevenue==null?"null":"" + BusinessFirmographicsParentRevenue, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessIndustrySector==null?"null":BusinessIndustrySector, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessRetirementParticipants==null?"null":"" + BusinessRetirementParticipants, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessSocialPresence==null?"null":BusinessSocialPresence, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessUrlNumPages==null?"null":"" + BusinessUrlNumPages, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessType==null?"null":BusinessType, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BusinessVCFunded==null?"null":BusinessVCFunded, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DerogatoryIndicator==null?"null":DerogatoryIndicator, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ExperianCreditRating==null?"null":ExperianCreditRating, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FundingAgency==null?"null":FundingAgency, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FundingAmount==null?"null":"" + FundingAmount, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FundingAwardAmount==null?"null":"" + FundingAwardAmount, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FundingFinanceRound==null?"null":"" + FundingFinanceRound, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FundingFiscalQuarter==null?"null":"" + FundingFiscalQuarter, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FundingFiscalYear==null?"null":"" + FundingFiscalYear, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FundingReceived==null?"null":"" + FundingReceived, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FundingStage==null?"null":FundingStage, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Intelliscore==null?"null":"" + Intelliscore, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(JobsRecentJobs==null?"null":"" + JobsRecentJobs, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(JobsTrendString==null?"null":JobsTrendString, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ModelAction==null?"null":ModelAction, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PercentileModel==null?"null":"" + PercentileModel, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RetirementAssetsEOY==null?"null":"" + RetirementAssetsEOY, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RetirementAssetsYOY==null?"null":"" + RetirementAssetsYOY, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TotalParticipantsSOY==null?"null":"" + TotalParticipantsSOY, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(UCCFilings==null?"null":"" + UCCFilings, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(UCCFilingsPresent==null?"null":UCCFilingsPresent, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Years_in_Business_Code==null?"null":Years_in_Business_Code, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Non_Profit_Indicator==null?"null":Non_Profit_Indicator, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PD_DA_AwardCategory==null?"null":PD_DA_AwardCategory, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PD_DA_JobTitle==null?"null":PD_DA_JobTitle, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PD_DA_LastSocialActivity_Units==null?"null":PD_DA_LastSocialActivity_Units, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PD_DA_MonthsPatentGranted==null?"null":"" + PD_DA_MonthsPatentGranted, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PD_DA_MonthsSinceFundAwardDate==null?"null":"" + PD_DA_MonthsSinceFundAwardDate, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PD_DA_PrimarySIC1==null?"null":PD_DA_PrimarySIC1, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Industry==null?"null":Industry, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(LeadSource==null?"null":LeadSource, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(AnnualRevenue==null?"null":"" + AnnualRevenue, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(NumberOfEmployees==null?"null":"" + NumberOfEmployees, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Alexa_MonthsSinceOnline==null?"null":"" + Alexa_MonthsSinceOnline, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Alexa_Rank==null?"null":"" + Alexa_Rank, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Alexa_ReachPerMillion==null?"null":"" + Alexa_ReachPerMillion, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Alexa_ViewsPerMillion==null?"null":"" + Alexa_ViewsPerMillion, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Alexa_ViewsPerUser==null?"null":"" + Alexa_ViewsPerUser, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_TechTags_Cnt==null?"null":"" + BW_TechTags_Cnt, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_TotalTech_Cnt==null?"null":"" + BW_TotalTech_Cnt, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_ads==null?"null":"" + BW_ads, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_analytics==null?"null":"" + BW_analytics, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_cdn==null?"null":"" + BW_cdn, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_cdns==null?"null":"" + BW_cdns, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_cms==null?"null":"" + BW_cms, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_docinfo==null?"null":"" + BW_docinfo, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_encoding==null?"null":"" + BW_encoding, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_feeds==null?"null":"" + BW_feeds, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_framework==null?"null":"" + BW_framework, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_hosting==null?"null":"" + BW_hosting, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_javascript==null?"null":"" + BW_javascript, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_mapping==null?"null":"" + BW_mapping, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_media==null?"null":"" + BW_media, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_mx==null?"null":"" + BW_mx, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_ns==null?"null":"" + BW_ns, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_parked==null?"null":"" + BW_parked, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_payment==null?"null":"" + BW_payment, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_seo_headers==null?"null":"" + BW_seo_headers, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_seo_meta==null?"null":"" + BW_seo_meta, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_seo_title==null?"null":"" + BW_seo_title, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_Server==null?"null":"" + BW_Server, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_shop==null?"null":"" + BW_shop, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_ssl==null?"null":"" + BW_ssl, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_Web_Master==null?"null":"" + BW_Web_Master, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_Web_Server==null?"null":"" + BW_Web_Server, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BW_widgets==null?"null":"" + BW_widgets, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Activity_ClickLink_cnt==null?"null":"" + Activity_ClickLink_cnt, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Activity_VisitWeb_cnt==null?"null":"" + Activity_VisitWeb_cnt, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Activity_InterestingMoment_cnt==null?"null":"" + Activity_InterestingMoment_cnt, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Activity_OpenEmail_cnt==null?"null":"" + Activity_OpenEmail_cnt, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Activity_EmailBncedSft_cnt==null?"null":"" + Activity_EmailBncedSft_cnt, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Activity_FillOutForm_cnt==null?"null":"" + Activity_FillOutForm_cnt, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Activity_UnsubscrbEmail_cnt==null?"null":"" + Activity_UnsubscrbEmail_cnt, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Activity_ClickEmail_cnt==null?"null":"" + Activity_ClickEmail_cnt, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_CloudService_One==null?"null":"" + CloudTechnologies_CloudService_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_CloudService_Two==null?"null":"" + CloudTechnologies_CloudService_Two, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_CommTech_One==null?"null":"" + CloudTechnologies_CommTech_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_CommTech_Two==null?"null":"" + CloudTechnologies_CommTech_Two, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_CRM_One==null?"null":"" + CloudTechnologies_CRM_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_CRM_Two==null?"null":"" + CloudTechnologies_CRM_Two, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_DataCenterSolutions_One==null?"null":"" + CloudTechnologies_DataCenterSolutions_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_DataCenterSolutions_Two==null?"null":"" + CloudTechnologies_DataCenterSolutions_Two, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_EnterpriseApplications_One==null?"null":"" + CloudTechnologies_EnterpriseApplications_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_EnterpriseApplications_Two==null?"null":"" + CloudTechnologies_EnterpriseApplications_Two, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_EnterpriseContent_One==null?"null":"" + CloudTechnologies_EnterpriseContent_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_EnterpriseContent_Two==null?"null":"" + CloudTechnologies_EnterpriseContent_Two, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_HardwareBasic_One==null?"null":"" + CloudTechnologies_HardwareBasic_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_HardwareBasic_Two==null?"null":"" + CloudTechnologies_HardwareBasic_Two, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_ITGovernance_One==null?"null":"" + CloudTechnologies_ITGovernance_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_ITGovernance_Two==null?"null":"" + CloudTechnologies_ITGovernance_Two, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_MarketingPerfMgmt_One==null?"null":"" + CloudTechnologies_MarketingPerfMgmt_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_MarketingPerfMgmt_Two==null?"null":"" + CloudTechnologies_MarketingPerfMgmt_Two, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_NetworkComputing_One==null?"null":"" + CloudTechnologies_NetworkComputing_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_NetworkComputing_Two==null?"null":"" + CloudTechnologies_NetworkComputing_Two, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_ProductivitySltns_One==null?"null":"" + CloudTechnologies_ProductivitySltns_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_ProductivitySltns_Two==null?"null":"" + CloudTechnologies_ProductivitySltns_Two, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_ProjectMgnt_One==null?"null":"" + CloudTechnologies_ProjectMgnt_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_ProjectMgnt_Two==null?"null":"" + CloudTechnologies_ProjectMgnt_Two, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_SoftwareBasic_One==null?"null":"" + CloudTechnologies_SoftwareBasic_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_SoftwareBasic_Two==null?"null":"" + CloudTechnologies_SoftwareBasic_Two, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_VerticalMarkets_One==null?"null":"" + CloudTechnologies_VerticalMarkets_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_VerticalMarkets_Two==null?"null":"" + CloudTechnologies_VerticalMarkets_Two, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_WebOrntdArch_One==null?"null":"" + CloudTechnologies_WebOrntdArch_One, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CloudTechnologies_WebOrntdArch_Two==null?"null":"" + CloudTechnologies_WebOrntdArch_Two, delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Nutanix_EventTable_Clean = null; } else {
      this.Nutanix_EventTable_Clean = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.LeadID = null; } else {
      this.LeadID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CustomerID = null; } else {
      this.CustomerID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.PeriodID = null; } else {
      this.PeriodID = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.P1_Target = null; } else {
      this.P1_Target = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.P1_TargetTraining = null; } else {
      this.P1_TargetTraining = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.P1_Event = null; } else {
      this.P1_Event = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.Company = null; } else {
      this.Company = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.Email = null; } else {
      this.Email = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.Domain = null; } else {
      this.Domain = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.AwardYear = null; } else {
      this.AwardYear = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BankruptcyFiled = null; } else {
      this.BankruptcyFiled = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BusinessAnnualSalesAbs = null; } else {
      this.BusinessAnnualSalesAbs = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BusinessAssets = null; } else {
      this.BusinessAssets = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BusinessECommerceSite = null; } else {
      this.BusinessECommerceSite = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BusinessEntityType = null; } else {
      this.BusinessEntityType = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BusinessEstablishedYear = null; } else {
      this.BusinessEstablishedYear = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BusinessEstimatedAnnualSales_k = null; } else {
      this.BusinessEstimatedAnnualSales_k = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BusinessEstimatedEmployees = null; } else {
      this.BusinessEstimatedEmployees = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BusinessFirmographicsParentEmployees = null; } else {
      this.BusinessFirmographicsParentEmployees = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BusinessFirmographicsParentRevenue = null; } else {
      this.BusinessFirmographicsParentRevenue = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BusinessIndustrySector = null; } else {
      this.BusinessIndustrySector = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BusinessRetirementParticipants = null; } else {
      this.BusinessRetirementParticipants = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BusinessSocialPresence = null; } else {
      this.BusinessSocialPresence = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BusinessUrlNumPages = null; } else {
      this.BusinessUrlNumPages = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BusinessType = null; } else {
      this.BusinessType = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BusinessVCFunded = null; } else {
      this.BusinessVCFunded = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DerogatoryIndicator = null; } else {
      this.DerogatoryIndicator = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ExperianCreditRating = null; } else {
      this.ExperianCreditRating = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FundingAgency = null; } else {
      this.FundingAgency = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FundingAmount = null; } else {
      this.FundingAmount = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FundingAwardAmount = null; } else {
      this.FundingAwardAmount = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FundingFinanceRound = null; } else {
      this.FundingFinanceRound = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FundingFiscalQuarter = null; } else {
      this.FundingFiscalQuarter = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FundingFiscalYear = null; } else {
      this.FundingFiscalYear = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FundingReceived = null; } else {
      this.FundingReceived = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FundingStage = null; } else {
      this.FundingStage = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Intelliscore = null; } else {
      this.Intelliscore = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.JobsRecentJobs = null; } else {
      this.JobsRecentJobs = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.JobsTrendString = null; } else {
      this.JobsTrendString = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ModelAction = null; } else {
      this.ModelAction = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.PercentileModel = null; } else {
      this.PercentileModel = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.RetirementAssetsEOY = null; } else {
      this.RetirementAssetsEOY = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.RetirementAssetsYOY = null; } else {
      this.RetirementAssetsYOY = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.TotalParticipantsSOY = null; } else {
      this.TotalParticipantsSOY = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.UCCFilings = null; } else {
      this.UCCFilings = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.UCCFilingsPresent = null; } else {
      this.UCCFilingsPresent = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.Years_in_Business_Code = null; } else {
      this.Years_in_Business_Code = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.Non_Profit_Indicator = null; } else {
      this.Non_Profit_Indicator = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PD_DA_AwardCategory = null; } else {
      this.PD_DA_AwardCategory = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PD_DA_JobTitle = null; } else {
      this.PD_DA_JobTitle = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PD_DA_LastSocialActivity_Units = null; } else {
      this.PD_DA_LastSocialActivity_Units = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.PD_DA_MonthsPatentGranted = null; } else {
      this.PD_DA_MonthsPatentGranted = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.PD_DA_MonthsSinceFundAwardDate = null; } else {
      this.PD_DA_MonthsSinceFundAwardDate = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PD_DA_PrimarySIC1 = null; } else {
      this.PD_DA_PrimarySIC1 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.Industry = null; } else {
      this.Industry = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.LeadSource = null; } else {
      this.LeadSource = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.AnnualRevenue = null; } else {
      this.AnnualRevenue = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.NumberOfEmployees = null; } else {
      this.NumberOfEmployees = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Alexa_MonthsSinceOnline = null; } else {
      this.Alexa_MonthsSinceOnline = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Alexa_Rank = null; } else {
      this.Alexa_Rank = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Alexa_ReachPerMillion = null; } else {
      this.Alexa_ReachPerMillion = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Alexa_ViewsPerMillion = null; } else {
      this.Alexa_ViewsPerMillion = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Alexa_ViewsPerUser = null; } else {
      this.Alexa_ViewsPerUser = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_TechTags_Cnt = null; } else {
      this.BW_TechTags_Cnt = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_TotalTech_Cnt = null; } else {
      this.BW_TotalTech_Cnt = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_ads = null; } else {
      this.BW_ads = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_analytics = null; } else {
      this.BW_analytics = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_cdn = null; } else {
      this.BW_cdn = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_cdns = null; } else {
      this.BW_cdns = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_cms = null; } else {
      this.BW_cms = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_docinfo = null; } else {
      this.BW_docinfo = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_encoding = null; } else {
      this.BW_encoding = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_feeds = null; } else {
      this.BW_feeds = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_framework = null; } else {
      this.BW_framework = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_hosting = null; } else {
      this.BW_hosting = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_javascript = null; } else {
      this.BW_javascript = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_mapping = null; } else {
      this.BW_mapping = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_media = null; } else {
      this.BW_media = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_mx = null; } else {
      this.BW_mx = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_ns = null; } else {
      this.BW_ns = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_parked = null; } else {
      this.BW_parked = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_payment = null; } else {
      this.BW_payment = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_seo_headers = null; } else {
      this.BW_seo_headers = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_seo_meta = null; } else {
      this.BW_seo_meta = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_seo_title = null; } else {
      this.BW_seo_title = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_Server = null; } else {
      this.BW_Server = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_shop = null; } else {
      this.BW_shop = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_ssl = null; } else {
      this.BW_ssl = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_Web_Master = null; } else {
      this.BW_Web_Master = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_Web_Server = null; } else {
      this.BW_Web_Server = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_widgets = null; } else {
      this.BW_widgets = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Activity_ClickLink_cnt = null; } else {
      this.Activity_ClickLink_cnt = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Activity_VisitWeb_cnt = null; } else {
      this.Activity_VisitWeb_cnt = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Activity_InterestingMoment_cnt = null; } else {
      this.Activity_InterestingMoment_cnt = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Activity_OpenEmail_cnt = null; } else {
      this.Activity_OpenEmail_cnt = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Activity_EmailBncedSft_cnt = null; } else {
      this.Activity_EmailBncedSft_cnt = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Activity_FillOutForm_cnt = null; } else {
      this.Activity_FillOutForm_cnt = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Activity_UnsubscrbEmail_cnt = null; } else {
      this.Activity_UnsubscrbEmail_cnt = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Activity_ClickEmail_cnt = null; } else {
      this.Activity_ClickEmail_cnt = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_CloudService_One = null; } else {
      this.CloudTechnologies_CloudService_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_CloudService_Two = null; } else {
      this.CloudTechnologies_CloudService_Two = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_CommTech_One = null; } else {
      this.CloudTechnologies_CommTech_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_CommTech_Two = null; } else {
      this.CloudTechnologies_CommTech_Two = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_CRM_One = null; } else {
      this.CloudTechnologies_CRM_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_CRM_Two = null; } else {
      this.CloudTechnologies_CRM_Two = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_DataCenterSolutions_One = null; } else {
      this.CloudTechnologies_DataCenterSolutions_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_DataCenterSolutions_Two = null; } else {
      this.CloudTechnologies_DataCenterSolutions_Two = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_EnterpriseApplications_One = null; } else {
      this.CloudTechnologies_EnterpriseApplications_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_EnterpriseApplications_Two = null; } else {
      this.CloudTechnologies_EnterpriseApplications_Two = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_EnterpriseContent_One = null; } else {
      this.CloudTechnologies_EnterpriseContent_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_EnterpriseContent_Two = null; } else {
      this.CloudTechnologies_EnterpriseContent_Two = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_HardwareBasic_One = null; } else {
      this.CloudTechnologies_HardwareBasic_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_HardwareBasic_Two = null; } else {
      this.CloudTechnologies_HardwareBasic_Two = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_ITGovernance_One = null; } else {
      this.CloudTechnologies_ITGovernance_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_ITGovernance_Two = null; } else {
      this.CloudTechnologies_ITGovernance_Two = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_MarketingPerfMgmt_One = null; } else {
      this.CloudTechnologies_MarketingPerfMgmt_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_MarketingPerfMgmt_Two = null; } else {
      this.CloudTechnologies_MarketingPerfMgmt_Two = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_NetworkComputing_One = null; } else {
      this.CloudTechnologies_NetworkComputing_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_NetworkComputing_Two = null; } else {
      this.CloudTechnologies_NetworkComputing_Two = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_ProductivitySltns_One = null; } else {
      this.CloudTechnologies_ProductivitySltns_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_ProductivitySltns_Two = null; } else {
      this.CloudTechnologies_ProductivitySltns_Two = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_ProjectMgnt_One = null; } else {
      this.CloudTechnologies_ProjectMgnt_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_ProjectMgnt_Two = null; } else {
      this.CloudTechnologies_ProjectMgnt_Two = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_SoftwareBasic_One = null; } else {
      this.CloudTechnologies_SoftwareBasic_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_SoftwareBasic_Two = null; } else {
      this.CloudTechnologies_SoftwareBasic_Two = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_VerticalMarkets_One = null; } else {
      this.CloudTechnologies_VerticalMarkets_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_VerticalMarkets_Two = null; } else {
      this.CloudTechnologies_VerticalMarkets_Two = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_WebOrntdArch_One = null; } else {
      this.CloudTechnologies_WebOrntdArch_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_WebOrntdArch_Two = null; } else {
      this.CloudTechnologies_WebOrntdArch_Two = Double.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Nutanix_EventTable_Clean = null; } else {
      this.Nutanix_EventTable_Clean = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.LeadID = null; } else {
      this.LeadID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CustomerID = null; } else {
      this.CustomerID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.PeriodID = null; } else {
      this.PeriodID = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.P1_Target = null; } else {
      this.P1_Target = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.P1_TargetTraining = null; } else {
      this.P1_TargetTraining = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.P1_Event = null; } else {
      this.P1_Event = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.Company = null; } else {
      this.Company = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.Email = null; } else {
      this.Email = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.Domain = null; } else {
      this.Domain = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.AwardYear = null; } else {
      this.AwardYear = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BankruptcyFiled = null; } else {
      this.BankruptcyFiled = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BusinessAnnualSalesAbs = null; } else {
      this.BusinessAnnualSalesAbs = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BusinessAssets = null; } else {
      this.BusinessAssets = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BusinessECommerceSite = null; } else {
      this.BusinessECommerceSite = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BusinessEntityType = null; } else {
      this.BusinessEntityType = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BusinessEstablishedYear = null; } else {
      this.BusinessEstablishedYear = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BusinessEstimatedAnnualSales_k = null; } else {
      this.BusinessEstimatedAnnualSales_k = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BusinessEstimatedEmployees = null; } else {
      this.BusinessEstimatedEmployees = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BusinessFirmographicsParentEmployees = null; } else {
      this.BusinessFirmographicsParentEmployees = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BusinessFirmographicsParentRevenue = null; } else {
      this.BusinessFirmographicsParentRevenue = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BusinessIndustrySector = null; } else {
      this.BusinessIndustrySector = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BusinessRetirementParticipants = null; } else {
      this.BusinessRetirementParticipants = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BusinessSocialPresence = null; } else {
      this.BusinessSocialPresence = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BusinessUrlNumPages = null; } else {
      this.BusinessUrlNumPages = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BusinessType = null; } else {
      this.BusinessType = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BusinessVCFunded = null; } else {
      this.BusinessVCFunded = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DerogatoryIndicator = null; } else {
      this.DerogatoryIndicator = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ExperianCreditRating = null; } else {
      this.ExperianCreditRating = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FundingAgency = null; } else {
      this.FundingAgency = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FundingAmount = null; } else {
      this.FundingAmount = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FundingAwardAmount = null; } else {
      this.FundingAwardAmount = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FundingFinanceRound = null; } else {
      this.FundingFinanceRound = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FundingFiscalQuarter = null; } else {
      this.FundingFiscalQuarter = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FundingFiscalYear = null; } else {
      this.FundingFiscalYear = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FundingReceived = null; } else {
      this.FundingReceived = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FundingStage = null; } else {
      this.FundingStage = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Intelliscore = null; } else {
      this.Intelliscore = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.JobsRecentJobs = null; } else {
      this.JobsRecentJobs = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.JobsTrendString = null; } else {
      this.JobsTrendString = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ModelAction = null; } else {
      this.ModelAction = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.PercentileModel = null; } else {
      this.PercentileModel = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.RetirementAssetsEOY = null; } else {
      this.RetirementAssetsEOY = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.RetirementAssetsYOY = null; } else {
      this.RetirementAssetsYOY = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.TotalParticipantsSOY = null; } else {
      this.TotalParticipantsSOY = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.UCCFilings = null; } else {
      this.UCCFilings = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.UCCFilingsPresent = null; } else {
      this.UCCFilingsPresent = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.Years_in_Business_Code = null; } else {
      this.Years_in_Business_Code = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.Non_Profit_Indicator = null; } else {
      this.Non_Profit_Indicator = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PD_DA_AwardCategory = null; } else {
      this.PD_DA_AwardCategory = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PD_DA_JobTitle = null; } else {
      this.PD_DA_JobTitle = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PD_DA_LastSocialActivity_Units = null; } else {
      this.PD_DA_LastSocialActivity_Units = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.PD_DA_MonthsPatentGranted = null; } else {
      this.PD_DA_MonthsPatentGranted = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.PD_DA_MonthsSinceFundAwardDate = null; } else {
      this.PD_DA_MonthsSinceFundAwardDate = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PD_DA_PrimarySIC1 = null; } else {
      this.PD_DA_PrimarySIC1 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.Industry = null; } else {
      this.Industry = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.LeadSource = null; } else {
      this.LeadSource = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.AnnualRevenue = null; } else {
      this.AnnualRevenue = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.NumberOfEmployees = null; } else {
      this.NumberOfEmployees = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Alexa_MonthsSinceOnline = null; } else {
      this.Alexa_MonthsSinceOnline = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Alexa_Rank = null; } else {
      this.Alexa_Rank = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Alexa_ReachPerMillion = null; } else {
      this.Alexa_ReachPerMillion = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Alexa_ViewsPerMillion = null; } else {
      this.Alexa_ViewsPerMillion = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Alexa_ViewsPerUser = null; } else {
      this.Alexa_ViewsPerUser = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_TechTags_Cnt = null; } else {
      this.BW_TechTags_Cnt = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_TotalTech_Cnt = null; } else {
      this.BW_TotalTech_Cnt = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_ads = null; } else {
      this.BW_ads = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_analytics = null; } else {
      this.BW_analytics = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_cdn = null; } else {
      this.BW_cdn = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_cdns = null; } else {
      this.BW_cdns = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_cms = null; } else {
      this.BW_cms = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_docinfo = null; } else {
      this.BW_docinfo = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_encoding = null; } else {
      this.BW_encoding = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_feeds = null; } else {
      this.BW_feeds = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_framework = null; } else {
      this.BW_framework = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_hosting = null; } else {
      this.BW_hosting = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_javascript = null; } else {
      this.BW_javascript = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_mapping = null; } else {
      this.BW_mapping = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_media = null; } else {
      this.BW_media = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_mx = null; } else {
      this.BW_mx = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_ns = null; } else {
      this.BW_ns = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_parked = null; } else {
      this.BW_parked = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_payment = null; } else {
      this.BW_payment = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_seo_headers = null; } else {
      this.BW_seo_headers = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_seo_meta = null; } else {
      this.BW_seo_meta = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_seo_title = null; } else {
      this.BW_seo_title = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_Server = null; } else {
      this.BW_Server = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_shop = null; } else {
      this.BW_shop = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_ssl = null; } else {
      this.BW_ssl = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_Web_Master = null; } else {
      this.BW_Web_Master = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_Web_Server = null; } else {
      this.BW_Web_Server = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BW_widgets = null; } else {
      this.BW_widgets = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Activity_ClickLink_cnt = null; } else {
      this.Activity_ClickLink_cnt = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Activity_VisitWeb_cnt = null; } else {
      this.Activity_VisitWeb_cnt = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Activity_InterestingMoment_cnt = null; } else {
      this.Activity_InterestingMoment_cnt = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Activity_OpenEmail_cnt = null; } else {
      this.Activity_OpenEmail_cnt = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Activity_EmailBncedSft_cnt = null; } else {
      this.Activity_EmailBncedSft_cnt = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Activity_FillOutForm_cnt = null; } else {
      this.Activity_FillOutForm_cnt = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Activity_UnsubscrbEmail_cnt = null; } else {
      this.Activity_UnsubscrbEmail_cnt = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Activity_ClickEmail_cnt = null; } else {
      this.Activity_ClickEmail_cnt = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_CloudService_One = null; } else {
      this.CloudTechnologies_CloudService_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_CloudService_Two = null; } else {
      this.CloudTechnologies_CloudService_Two = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_CommTech_One = null; } else {
      this.CloudTechnologies_CommTech_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_CommTech_Two = null; } else {
      this.CloudTechnologies_CommTech_Two = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_CRM_One = null; } else {
      this.CloudTechnologies_CRM_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_CRM_Two = null; } else {
      this.CloudTechnologies_CRM_Two = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_DataCenterSolutions_One = null; } else {
      this.CloudTechnologies_DataCenterSolutions_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_DataCenterSolutions_Two = null; } else {
      this.CloudTechnologies_DataCenterSolutions_Two = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_EnterpriseApplications_One = null; } else {
      this.CloudTechnologies_EnterpriseApplications_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_EnterpriseApplications_Two = null; } else {
      this.CloudTechnologies_EnterpriseApplications_Two = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_EnterpriseContent_One = null; } else {
      this.CloudTechnologies_EnterpriseContent_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_EnterpriseContent_Two = null; } else {
      this.CloudTechnologies_EnterpriseContent_Two = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_HardwareBasic_One = null; } else {
      this.CloudTechnologies_HardwareBasic_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_HardwareBasic_Two = null; } else {
      this.CloudTechnologies_HardwareBasic_Two = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_ITGovernance_One = null; } else {
      this.CloudTechnologies_ITGovernance_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_ITGovernance_Two = null; } else {
      this.CloudTechnologies_ITGovernance_Two = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_MarketingPerfMgmt_One = null; } else {
      this.CloudTechnologies_MarketingPerfMgmt_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_MarketingPerfMgmt_Two = null; } else {
      this.CloudTechnologies_MarketingPerfMgmt_Two = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_NetworkComputing_One = null; } else {
      this.CloudTechnologies_NetworkComputing_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_NetworkComputing_Two = null; } else {
      this.CloudTechnologies_NetworkComputing_Two = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_ProductivitySltns_One = null; } else {
      this.CloudTechnologies_ProductivitySltns_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_ProductivitySltns_Two = null; } else {
      this.CloudTechnologies_ProductivitySltns_Two = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_ProjectMgnt_One = null; } else {
      this.CloudTechnologies_ProjectMgnt_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_ProjectMgnt_Two = null; } else {
      this.CloudTechnologies_ProjectMgnt_Two = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_SoftwareBasic_One = null; } else {
      this.CloudTechnologies_SoftwareBasic_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_SoftwareBasic_Two = null; } else {
      this.CloudTechnologies_SoftwareBasic_Two = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_VerticalMarkets_One = null; } else {
      this.CloudTechnologies_VerticalMarkets_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_VerticalMarkets_Two = null; } else {
      this.CloudTechnologies_VerticalMarkets_Two = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_WebOrntdArch_One = null; } else {
      this.CloudTechnologies_WebOrntdArch_One = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CloudTechnologies_WebOrntdArch_Two = null; } else {
      this.CloudTechnologies_WebOrntdArch_Two = Double.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    Nutanix o = (Nutanix) super.clone();
    return o;
  }

  public void clone0(Nutanix o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new TreeMap<String, Object>();
    __sqoop$field_map.put("Nutanix_EventTable_Clean", this.Nutanix_EventTable_Clean);
    __sqoop$field_map.put("LeadID", this.LeadID);
    __sqoop$field_map.put("CustomerID", this.CustomerID);
    __sqoop$field_map.put("PeriodID", this.PeriodID);
    __sqoop$field_map.put("P1_Target", this.P1_Target);
    __sqoop$field_map.put("P1_TargetTraining", this.P1_TargetTraining);
    __sqoop$field_map.put("P1_Event", this.P1_Event);
    __sqoop$field_map.put("Company", this.Company);
    __sqoop$field_map.put("Email", this.Email);
    __sqoop$field_map.put("Domain", this.Domain);
    __sqoop$field_map.put("AwardYear", this.AwardYear);
    __sqoop$field_map.put("BankruptcyFiled", this.BankruptcyFiled);
    __sqoop$field_map.put("BusinessAnnualSalesAbs", this.BusinessAnnualSalesAbs);
    __sqoop$field_map.put("BusinessAssets", this.BusinessAssets);
    __sqoop$field_map.put("BusinessECommerceSite", this.BusinessECommerceSite);
    __sqoop$field_map.put("BusinessEntityType", this.BusinessEntityType);
    __sqoop$field_map.put("BusinessEstablishedYear", this.BusinessEstablishedYear);
    __sqoop$field_map.put("BusinessEstimatedAnnualSales_k", this.BusinessEstimatedAnnualSales_k);
    __sqoop$field_map.put("BusinessEstimatedEmployees", this.BusinessEstimatedEmployees);
    __sqoop$field_map.put("BusinessFirmographicsParentEmployees", this.BusinessFirmographicsParentEmployees);
    __sqoop$field_map.put("BusinessFirmographicsParentRevenue", this.BusinessFirmographicsParentRevenue);
    __sqoop$field_map.put("BusinessIndustrySector", this.BusinessIndustrySector);
    __sqoop$field_map.put("BusinessRetirementParticipants", this.BusinessRetirementParticipants);
    __sqoop$field_map.put("BusinessSocialPresence", this.BusinessSocialPresence);
    __sqoop$field_map.put("BusinessUrlNumPages", this.BusinessUrlNumPages);
    __sqoop$field_map.put("BusinessType", this.BusinessType);
    __sqoop$field_map.put("BusinessVCFunded", this.BusinessVCFunded);
    __sqoop$field_map.put("DerogatoryIndicator", this.DerogatoryIndicator);
    __sqoop$field_map.put("ExperianCreditRating", this.ExperianCreditRating);
    __sqoop$field_map.put("FundingAgency", this.FundingAgency);
    __sqoop$field_map.put("FundingAmount", this.FundingAmount);
    __sqoop$field_map.put("FundingAwardAmount", this.FundingAwardAmount);
    __sqoop$field_map.put("FundingFinanceRound", this.FundingFinanceRound);
    __sqoop$field_map.put("FundingFiscalQuarter", this.FundingFiscalQuarter);
    __sqoop$field_map.put("FundingFiscalYear", this.FundingFiscalYear);
    __sqoop$field_map.put("FundingReceived", this.FundingReceived);
    __sqoop$field_map.put("FundingStage", this.FundingStage);
    __sqoop$field_map.put("Intelliscore", this.Intelliscore);
    __sqoop$field_map.put("JobsRecentJobs", this.JobsRecentJobs);
    __sqoop$field_map.put("JobsTrendString", this.JobsTrendString);
    __sqoop$field_map.put("ModelAction", this.ModelAction);
    __sqoop$field_map.put("PercentileModel", this.PercentileModel);
    __sqoop$field_map.put("RetirementAssetsEOY", this.RetirementAssetsEOY);
    __sqoop$field_map.put("RetirementAssetsYOY", this.RetirementAssetsYOY);
    __sqoop$field_map.put("TotalParticipantsSOY", this.TotalParticipantsSOY);
    __sqoop$field_map.put("UCCFilings", this.UCCFilings);
    __sqoop$field_map.put("UCCFilingsPresent", this.UCCFilingsPresent);
    __sqoop$field_map.put("Years_in_Business_Code", this.Years_in_Business_Code);
    __sqoop$field_map.put("Non_Profit_Indicator", this.Non_Profit_Indicator);
    __sqoop$field_map.put("PD_DA_AwardCategory", this.PD_DA_AwardCategory);
    __sqoop$field_map.put("PD_DA_JobTitle", this.PD_DA_JobTitle);
    __sqoop$field_map.put("PD_DA_LastSocialActivity_Units", this.PD_DA_LastSocialActivity_Units);
    __sqoop$field_map.put("PD_DA_MonthsPatentGranted", this.PD_DA_MonthsPatentGranted);
    __sqoop$field_map.put("PD_DA_MonthsSinceFundAwardDate", this.PD_DA_MonthsSinceFundAwardDate);
    __sqoop$field_map.put("PD_DA_PrimarySIC1", this.PD_DA_PrimarySIC1);
    __sqoop$field_map.put("Industry", this.Industry);
    __sqoop$field_map.put("LeadSource", this.LeadSource);
    __sqoop$field_map.put("AnnualRevenue", this.AnnualRevenue);
    __sqoop$field_map.put("NumberOfEmployees", this.NumberOfEmployees);
    __sqoop$field_map.put("Alexa_MonthsSinceOnline", this.Alexa_MonthsSinceOnline);
    __sqoop$field_map.put("Alexa_Rank", this.Alexa_Rank);
    __sqoop$field_map.put("Alexa_ReachPerMillion", this.Alexa_ReachPerMillion);
    __sqoop$field_map.put("Alexa_ViewsPerMillion", this.Alexa_ViewsPerMillion);
    __sqoop$field_map.put("Alexa_ViewsPerUser", this.Alexa_ViewsPerUser);
    __sqoop$field_map.put("BW_TechTags_Cnt", this.BW_TechTags_Cnt);
    __sqoop$field_map.put("BW_TotalTech_Cnt", this.BW_TotalTech_Cnt);
    __sqoop$field_map.put("BW_ads", this.BW_ads);
    __sqoop$field_map.put("BW_analytics", this.BW_analytics);
    __sqoop$field_map.put("BW_cdn", this.BW_cdn);
    __sqoop$field_map.put("BW_cdns", this.BW_cdns);
    __sqoop$field_map.put("BW_cms", this.BW_cms);
    __sqoop$field_map.put("BW_docinfo", this.BW_docinfo);
    __sqoop$field_map.put("BW_encoding", this.BW_encoding);
    __sqoop$field_map.put("BW_feeds", this.BW_feeds);
    __sqoop$field_map.put("BW_framework", this.BW_framework);
    __sqoop$field_map.put("BW_hosting", this.BW_hosting);
    __sqoop$field_map.put("BW_javascript", this.BW_javascript);
    __sqoop$field_map.put("BW_mapping", this.BW_mapping);
    __sqoop$field_map.put("BW_media", this.BW_media);
    __sqoop$field_map.put("BW_mx", this.BW_mx);
    __sqoop$field_map.put("BW_ns", this.BW_ns);
    __sqoop$field_map.put("BW_parked", this.BW_parked);
    __sqoop$field_map.put("BW_payment", this.BW_payment);
    __sqoop$field_map.put("BW_seo_headers", this.BW_seo_headers);
    __sqoop$field_map.put("BW_seo_meta", this.BW_seo_meta);
    __sqoop$field_map.put("BW_seo_title", this.BW_seo_title);
    __sqoop$field_map.put("BW_Server", this.BW_Server);
    __sqoop$field_map.put("BW_shop", this.BW_shop);
    __sqoop$field_map.put("BW_ssl", this.BW_ssl);
    __sqoop$field_map.put("BW_Web_Master", this.BW_Web_Master);
    __sqoop$field_map.put("BW_Web_Server", this.BW_Web_Server);
    __sqoop$field_map.put("BW_widgets", this.BW_widgets);
    __sqoop$field_map.put("Activity_ClickLink_cnt", this.Activity_ClickLink_cnt);
    __sqoop$field_map.put("Activity_VisitWeb_cnt", this.Activity_VisitWeb_cnt);
    __sqoop$field_map.put("Activity_InterestingMoment_cnt", this.Activity_InterestingMoment_cnt);
    __sqoop$field_map.put("Activity_OpenEmail_cnt", this.Activity_OpenEmail_cnt);
    __sqoop$field_map.put("Activity_EmailBncedSft_cnt", this.Activity_EmailBncedSft_cnt);
    __sqoop$field_map.put("Activity_FillOutForm_cnt", this.Activity_FillOutForm_cnt);
    __sqoop$field_map.put("Activity_UnsubscrbEmail_cnt", this.Activity_UnsubscrbEmail_cnt);
    __sqoop$field_map.put("Activity_ClickEmail_cnt", this.Activity_ClickEmail_cnt);
    __sqoop$field_map.put("CloudTechnologies_CloudService_One", this.CloudTechnologies_CloudService_One);
    __sqoop$field_map.put("CloudTechnologies_CloudService_Two", this.CloudTechnologies_CloudService_Two);
    __sqoop$field_map.put("CloudTechnologies_CommTech_One", this.CloudTechnologies_CommTech_One);
    __sqoop$field_map.put("CloudTechnologies_CommTech_Two", this.CloudTechnologies_CommTech_Two);
    __sqoop$field_map.put("CloudTechnologies_CRM_One", this.CloudTechnologies_CRM_One);
    __sqoop$field_map.put("CloudTechnologies_CRM_Two", this.CloudTechnologies_CRM_Two);
    __sqoop$field_map.put("CloudTechnologies_DataCenterSolutions_One", this.CloudTechnologies_DataCenterSolutions_One);
    __sqoop$field_map.put("CloudTechnologies_DataCenterSolutions_Two", this.CloudTechnologies_DataCenterSolutions_Two);
    __sqoop$field_map.put("CloudTechnologies_EnterpriseApplications_One", this.CloudTechnologies_EnterpriseApplications_One);
    __sqoop$field_map.put("CloudTechnologies_EnterpriseApplications_Two", this.CloudTechnologies_EnterpriseApplications_Two);
    __sqoop$field_map.put("CloudTechnologies_EnterpriseContent_One", this.CloudTechnologies_EnterpriseContent_One);
    __sqoop$field_map.put("CloudTechnologies_EnterpriseContent_Two", this.CloudTechnologies_EnterpriseContent_Two);
    __sqoop$field_map.put("CloudTechnologies_HardwareBasic_One", this.CloudTechnologies_HardwareBasic_One);
    __sqoop$field_map.put("CloudTechnologies_HardwareBasic_Two", this.CloudTechnologies_HardwareBasic_Two);
    __sqoop$field_map.put("CloudTechnologies_ITGovernance_One", this.CloudTechnologies_ITGovernance_One);
    __sqoop$field_map.put("CloudTechnologies_ITGovernance_Two", this.CloudTechnologies_ITGovernance_Two);
    __sqoop$field_map.put("CloudTechnologies_MarketingPerfMgmt_One", this.CloudTechnologies_MarketingPerfMgmt_One);
    __sqoop$field_map.put("CloudTechnologies_MarketingPerfMgmt_Two", this.CloudTechnologies_MarketingPerfMgmt_Two);
    __sqoop$field_map.put("CloudTechnologies_NetworkComputing_One", this.CloudTechnologies_NetworkComputing_One);
    __sqoop$field_map.put("CloudTechnologies_NetworkComputing_Two", this.CloudTechnologies_NetworkComputing_Two);
    __sqoop$field_map.put("CloudTechnologies_ProductivitySltns_One", this.CloudTechnologies_ProductivitySltns_One);
    __sqoop$field_map.put("CloudTechnologies_ProductivitySltns_Two", this.CloudTechnologies_ProductivitySltns_Two);
    __sqoop$field_map.put("CloudTechnologies_ProjectMgnt_One", this.CloudTechnologies_ProjectMgnt_One);
    __sqoop$field_map.put("CloudTechnologies_ProjectMgnt_Two", this.CloudTechnologies_ProjectMgnt_Two);
    __sqoop$field_map.put("CloudTechnologies_SoftwareBasic_One", this.CloudTechnologies_SoftwareBasic_One);
    __sqoop$field_map.put("CloudTechnologies_SoftwareBasic_Two", this.CloudTechnologies_SoftwareBasic_Two);
    __sqoop$field_map.put("CloudTechnologies_VerticalMarkets_One", this.CloudTechnologies_VerticalMarkets_One);
    __sqoop$field_map.put("CloudTechnologies_VerticalMarkets_Two", this.CloudTechnologies_VerticalMarkets_Two);
    __sqoop$field_map.put("CloudTechnologies_WebOrntdArch_One", this.CloudTechnologies_WebOrntdArch_One);
    __sqoop$field_map.put("CloudTechnologies_WebOrntdArch_Two", this.CloudTechnologies_WebOrntdArch_Two);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("Nutanix_EventTable_Clean", this.Nutanix_EventTable_Clean);
    __sqoop$field_map.put("LeadID", this.LeadID);
    __sqoop$field_map.put("CustomerID", this.CustomerID);
    __sqoop$field_map.put("PeriodID", this.PeriodID);
    __sqoop$field_map.put("P1_Target", this.P1_Target);
    __sqoop$field_map.put("P1_TargetTraining", this.P1_TargetTraining);
    __sqoop$field_map.put("P1_Event", this.P1_Event);
    __sqoop$field_map.put("Company", this.Company);
    __sqoop$field_map.put("Email", this.Email);
    __sqoop$field_map.put("Domain", this.Domain);
    __sqoop$field_map.put("AwardYear", this.AwardYear);
    __sqoop$field_map.put("BankruptcyFiled", this.BankruptcyFiled);
    __sqoop$field_map.put("BusinessAnnualSalesAbs", this.BusinessAnnualSalesAbs);
    __sqoop$field_map.put("BusinessAssets", this.BusinessAssets);
    __sqoop$field_map.put("BusinessECommerceSite", this.BusinessECommerceSite);
    __sqoop$field_map.put("BusinessEntityType", this.BusinessEntityType);
    __sqoop$field_map.put("BusinessEstablishedYear", this.BusinessEstablishedYear);
    __sqoop$field_map.put("BusinessEstimatedAnnualSales_k", this.BusinessEstimatedAnnualSales_k);
    __sqoop$field_map.put("BusinessEstimatedEmployees", this.BusinessEstimatedEmployees);
    __sqoop$field_map.put("BusinessFirmographicsParentEmployees", this.BusinessFirmographicsParentEmployees);
    __sqoop$field_map.put("BusinessFirmographicsParentRevenue", this.BusinessFirmographicsParentRevenue);
    __sqoop$field_map.put("BusinessIndustrySector", this.BusinessIndustrySector);
    __sqoop$field_map.put("BusinessRetirementParticipants", this.BusinessRetirementParticipants);
    __sqoop$field_map.put("BusinessSocialPresence", this.BusinessSocialPresence);
    __sqoop$field_map.put("BusinessUrlNumPages", this.BusinessUrlNumPages);
    __sqoop$field_map.put("BusinessType", this.BusinessType);
    __sqoop$field_map.put("BusinessVCFunded", this.BusinessVCFunded);
    __sqoop$field_map.put("DerogatoryIndicator", this.DerogatoryIndicator);
    __sqoop$field_map.put("ExperianCreditRating", this.ExperianCreditRating);
    __sqoop$field_map.put("FundingAgency", this.FundingAgency);
    __sqoop$field_map.put("FundingAmount", this.FundingAmount);
    __sqoop$field_map.put("FundingAwardAmount", this.FundingAwardAmount);
    __sqoop$field_map.put("FundingFinanceRound", this.FundingFinanceRound);
    __sqoop$field_map.put("FundingFiscalQuarter", this.FundingFiscalQuarter);
    __sqoop$field_map.put("FundingFiscalYear", this.FundingFiscalYear);
    __sqoop$field_map.put("FundingReceived", this.FundingReceived);
    __sqoop$field_map.put("FundingStage", this.FundingStage);
    __sqoop$field_map.put("Intelliscore", this.Intelliscore);
    __sqoop$field_map.put("JobsRecentJobs", this.JobsRecentJobs);
    __sqoop$field_map.put("JobsTrendString", this.JobsTrendString);
    __sqoop$field_map.put("ModelAction", this.ModelAction);
    __sqoop$field_map.put("PercentileModel", this.PercentileModel);
    __sqoop$field_map.put("RetirementAssetsEOY", this.RetirementAssetsEOY);
    __sqoop$field_map.put("RetirementAssetsYOY", this.RetirementAssetsYOY);
    __sqoop$field_map.put("TotalParticipantsSOY", this.TotalParticipantsSOY);
    __sqoop$field_map.put("UCCFilings", this.UCCFilings);
    __sqoop$field_map.put("UCCFilingsPresent", this.UCCFilingsPresent);
    __sqoop$field_map.put("Years_in_Business_Code", this.Years_in_Business_Code);
    __sqoop$field_map.put("Non_Profit_Indicator", this.Non_Profit_Indicator);
    __sqoop$field_map.put("PD_DA_AwardCategory", this.PD_DA_AwardCategory);
    __sqoop$field_map.put("PD_DA_JobTitle", this.PD_DA_JobTitle);
    __sqoop$field_map.put("PD_DA_LastSocialActivity_Units", this.PD_DA_LastSocialActivity_Units);
    __sqoop$field_map.put("PD_DA_MonthsPatentGranted", this.PD_DA_MonthsPatentGranted);
    __sqoop$field_map.put("PD_DA_MonthsSinceFundAwardDate", this.PD_DA_MonthsSinceFundAwardDate);
    __sqoop$field_map.put("PD_DA_PrimarySIC1", this.PD_DA_PrimarySIC1);
    __sqoop$field_map.put("Industry", this.Industry);
    __sqoop$field_map.put("LeadSource", this.LeadSource);
    __sqoop$field_map.put("AnnualRevenue", this.AnnualRevenue);
    __sqoop$field_map.put("NumberOfEmployees", this.NumberOfEmployees);
    __sqoop$field_map.put("Alexa_MonthsSinceOnline", this.Alexa_MonthsSinceOnline);
    __sqoop$field_map.put("Alexa_Rank", this.Alexa_Rank);
    __sqoop$field_map.put("Alexa_ReachPerMillion", this.Alexa_ReachPerMillion);
    __sqoop$field_map.put("Alexa_ViewsPerMillion", this.Alexa_ViewsPerMillion);
    __sqoop$field_map.put("Alexa_ViewsPerUser", this.Alexa_ViewsPerUser);
    __sqoop$field_map.put("BW_TechTags_Cnt", this.BW_TechTags_Cnt);
    __sqoop$field_map.put("BW_TotalTech_Cnt", this.BW_TotalTech_Cnt);
    __sqoop$field_map.put("BW_ads", this.BW_ads);
    __sqoop$field_map.put("BW_analytics", this.BW_analytics);
    __sqoop$field_map.put("BW_cdn", this.BW_cdn);
    __sqoop$field_map.put("BW_cdns", this.BW_cdns);
    __sqoop$field_map.put("BW_cms", this.BW_cms);
    __sqoop$field_map.put("BW_docinfo", this.BW_docinfo);
    __sqoop$field_map.put("BW_encoding", this.BW_encoding);
    __sqoop$field_map.put("BW_feeds", this.BW_feeds);
    __sqoop$field_map.put("BW_framework", this.BW_framework);
    __sqoop$field_map.put("BW_hosting", this.BW_hosting);
    __sqoop$field_map.put("BW_javascript", this.BW_javascript);
    __sqoop$field_map.put("BW_mapping", this.BW_mapping);
    __sqoop$field_map.put("BW_media", this.BW_media);
    __sqoop$field_map.put("BW_mx", this.BW_mx);
    __sqoop$field_map.put("BW_ns", this.BW_ns);
    __sqoop$field_map.put("BW_parked", this.BW_parked);
    __sqoop$field_map.put("BW_payment", this.BW_payment);
    __sqoop$field_map.put("BW_seo_headers", this.BW_seo_headers);
    __sqoop$field_map.put("BW_seo_meta", this.BW_seo_meta);
    __sqoop$field_map.put("BW_seo_title", this.BW_seo_title);
    __sqoop$field_map.put("BW_Server", this.BW_Server);
    __sqoop$field_map.put("BW_shop", this.BW_shop);
    __sqoop$field_map.put("BW_ssl", this.BW_ssl);
    __sqoop$field_map.put("BW_Web_Master", this.BW_Web_Master);
    __sqoop$field_map.put("BW_Web_Server", this.BW_Web_Server);
    __sqoop$field_map.put("BW_widgets", this.BW_widgets);
    __sqoop$field_map.put("Activity_ClickLink_cnt", this.Activity_ClickLink_cnt);
    __sqoop$field_map.put("Activity_VisitWeb_cnt", this.Activity_VisitWeb_cnt);
    __sqoop$field_map.put("Activity_InterestingMoment_cnt", this.Activity_InterestingMoment_cnt);
    __sqoop$field_map.put("Activity_OpenEmail_cnt", this.Activity_OpenEmail_cnt);
    __sqoop$field_map.put("Activity_EmailBncedSft_cnt", this.Activity_EmailBncedSft_cnt);
    __sqoop$field_map.put("Activity_FillOutForm_cnt", this.Activity_FillOutForm_cnt);
    __sqoop$field_map.put("Activity_UnsubscrbEmail_cnt", this.Activity_UnsubscrbEmail_cnt);
    __sqoop$field_map.put("Activity_ClickEmail_cnt", this.Activity_ClickEmail_cnt);
    __sqoop$field_map.put("CloudTechnologies_CloudService_One", this.CloudTechnologies_CloudService_One);
    __sqoop$field_map.put("CloudTechnologies_CloudService_Two", this.CloudTechnologies_CloudService_Two);
    __sqoop$field_map.put("CloudTechnologies_CommTech_One", this.CloudTechnologies_CommTech_One);
    __sqoop$field_map.put("CloudTechnologies_CommTech_Two", this.CloudTechnologies_CommTech_Two);
    __sqoop$field_map.put("CloudTechnologies_CRM_One", this.CloudTechnologies_CRM_One);
    __sqoop$field_map.put("CloudTechnologies_CRM_Two", this.CloudTechnologies_CRM_Two);
    __sqoop$field_map.put("CloudTechnologies_DataCenterSolutions_One", this.CloudTechnologies_DataCenterSolutions_One);
    __sqoop$field_map.put("CloudTechnologies_DataCenterSolutions_Two", this.CloudTechnologies_DataCenterSolutions_Two);
    __sqoop$field_map.put("CloudTechnologies_EnterpriseApplications_One", this.CloudTechnologies_EnterpriseApplications_One);
    __sqoop$field_map.put("CloudTechnologies_EnterpriseApplications_Two", this.CloudTechnologies_EnterpriseApplications_Two);
    __sqoop$field_map.put("CloudTechnologies_EnterpriseContent_One", this.CloudTechnologies_EnterpriseContent_One);
    __sqoop$field_map.put("CloudTechnologies_EnterpriseContent_Two", this.CloudTechnologies_EnterpriseContent_Two);
    __sqoop$field_map.put("CloudTechnologies_HardwareBasic_One", this.CloudTechnologies_HardwareBasic_One);
    __sqoop$field_map.put("CloudTechnologies_HardwareBasic_Two", this.CloudTechnologies_HardwareBasic_Two);
    __sqoop$field_map.put("CloudTechnologies_ITGovernance_One", this.CloudTechnologies_ITGovernance_One);
    __sqoop$field_map.put("CloudTechnologies_ITGovernance_Two", this.CloudTechnologies_ITGovernance_Two);
    __sqoop$field_map.put("CloudTechnologies_MarketingPerfMgmt_One", this.CloudTechnologies_MarketingPerfMgmt_One);
    __sqoop$field_map.put("CloudTechnologies_MarketingPerfMgmt_Two", this.CloudTechnologies_MarketingPerfMgmt_Two);
    __sqoop$field_map.put("CloudTechnologies_NetworkComputing_One", this.CloudTechnologies_NetworkComputing_One);
    __sqoop$field_map.put("CloudTechnologies_NetworkComputing_Two", this.CloudTechnologies_NetworkComputing_Two);
    __sqoop$field_map.put("CloudTechnologies_ProductivitySltns_One", this.CloudTechnologies_ProductivitySltns_One);
    __sqoop$field_map.put("CloudTechnologies_ProductivitySltns_Two", this.CloudTechnologies_ProductivitySltns_Two);
    __sqoop$field_map.put("CloudTechnologies_ProjectMgnt_One", this.CloudTechnologies_ProjectMgnt_One);
    __sqoop$field_map.put("CloudTechnologies_ProjectMgnt_Two", this.CloudTechnologies_ProjectMgnt_Two);
    __sqoop$field_map.put("CloudTechnologies_SoftwareBasic_One", this.CloudTechnologies_SoftwareBasic_One);
    __sqoop$field_map.put("CloudTechnologies_SoftwareBasic_Two", this.CloudTechnologies_SoftwareBasic_Two);
    __sqoop$field_map.put("CloudTechnologies_VerticalMarkets_One", this.CloudTechnologies_VerticalMarkets_One);
    __sqoop$field_map.put("CloudTechnologies_VerticalMarkets_Two", this.CloudTechnologies_VerticalMarkets_Two);
    __sqoop$field_map.put("CloudTechnologies_WebOrntdArch_One", this.CloudTechnologies_WebOrntdArch_One);
    __sqoop$field_map.put("CloudTechnologies_WebOrntdArch_Two", this.CloudTechnologies_WebOrntdArch_Two);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if ("Nutanix_EventTable_Clean".equals(__fieldName)) {
      this.Nutanix_EventTable_Clean = (Long) __fieldVal;
    }
    else    if ("LeadID".equals(__fieldName)) {
      this.LeadID = (String) __fieldVal;
    }
    else    if ("CustomerID".equals(__fieldName)) {
      this.CustomerID = (String) __fieldVal;
    }
    else    if ("PeriodID".equals(__fieldName)) {
      this.PeriodID = (Long) __fieldVal;
    }
    else    if ("P1_Target".equals(__fieldName)) {
      this.P1_Target = (Long) __fieldVal;
    }
    else    if ("P1_TargetTraining".equals(__fieldName)) {
      this.P1_TargetTraining = (Long) __fieldVal;
    }
    else    if ("P1_Event".equals(__fieldName)) {
      this.P1_Event = (String) __fieldVal;
    }
    else    if ("Company".equals(__fieldName)) {
      this.Company = (String) __fieldVal;
    }
    else    if ("Email".equals(__fieldName)) {
      this.Email = (String) __fieldVal;
    }
    else    if ("Domain".equals(__fieldName)) {
      this.Domain = (String) __fieldVal;
    }
    else    if ("AwardYear".equals(__fieldName)) {
      this.AwardYear = (Long) __fieldVal;
    }
    else    if ("BankruptcyFiled".equals(__fieldName)) {
      this.BankruptcyFiled = (String) __fieldVal;
    }
    else    if ("BusinessAnnualSalesAbs".equals(__fieldName)) {
      this.BusinessAnnualSalesAbs = (Double) __fieldVal;
    }
    else    if ("BusinessAssets".equals(__fieldName)) {
      this.BusinessAssets = (String) __fieldVal;
    }
    else    if ("BusinessECommerceSite".equals(__fieldName)) {
      this.BusinessECommerceSite = (String) __fieldVal;
    }
    else    if ("BusinessEntityType".equals(__fieldName)) {
      this.BusinessEntityType = (String) __fieldVal;
    }
    else    if ("BusinessEstablishedYear".equals(__fieldName)) {
      this.BusinessEstablishedYear = (Double) __fieldVal;
    }
    else    if ("BusinessEstimatedAnnualSales_k".equals(__fieldName)) {
      this.BusinessEstimatedAnnualSales_k = (Double) __fieldVal;
    }
    else    if ("BusinessEstimatedEmployees".equals(__fieldName)) {
      this.BusinessEstimatedEmployees = (Double) __fieldVal;
    }
    else    if ("BusinessFirmographicsParentEmployees".equals(__fieldName)) {
      this.BusinessFirmographicsParentEmployees = (Double) __fieldVal;
    }
    else    if ("BusinessFirmographicsParentRevenue".equals(__fieldName)) {
      this.BusinessFirmographicsParentRevenue = (Double) __fieldVal;
    }
    else    if ("BusinessIndustrySector".equals(__fieldName)) {
      this.BusinessIndustrySector = (String) __fieldVal;
    }
    else    if ("BusinessRetirementParticipants".equals(__fieldName)) {
      this.BusinessRetirementParticipants = (Double) __fieldVal;
    }
    else    if ("BusinessSocialPresence".equals(__fieldName)) {
      this.BusinessSocialPresence = (String) __fieldVal;
    }
    else    if ("BusinessUrlNumPages".equals(__fieldName)) {
      this.BusinessUrlNumPages = (Double) __fieldVal;
    }
    else    if ("BusinessType".equals(__fieldName)) {
      this.BusinessType = (String) __fieldVal;
    }
    else    if ("BusinessVCFunded".equals(__fieldName)) {
      this.BusinessVCFunded = (String) __fieldVal;
    }
    else    if ("DerogatoryIndicator".equals(__fieldName)) {
      this.DerogatoryIndicator = (String) __fieldVal;
    }
    else    if ("ExperianCreditRating".equals(__fieldName)) {
      this.ExperianCreditRating = (String) __fieldVal;
    }
    else    if ("FundingAgency".equals(__fieldName)) {
      this.FundingAgency = (String) __fieldVal;
    }
    else    if ("FundingAmount".equals(__fieldName)) {
      this.FundingAmount = (Double) __fieldVal;
    }
    else    if ("FundingAwardAmount".equals(__fieldName)) {
      this.FundingAwardAmount = (Double) __fieldVal;
    }
    else    if ("FundingFinanceRound".equals(__fieldName)) {
      this.FundingFinanceRound = (Double) __fieldVal;
    }
    else    if ("FundingFiscalQuarter".equals(__fieldName)) {
      this.FundingFiscalQuarter = (Double) __fieldVal;
    }
    else    if ("FundingFiscalYear".equals(__fieldName)) {
      this.FundingFiscalYear = (Double) __fieldVal;
    }
    else    if ("FundingReceived".equals(__fieldName)) {
      this.FundingReceived = (Double) __fieldVal;
    }
    else    if ("FundingStage".equals(__fieldName)) {
      this.FundingStage = (String) __fieldVal;
    }
    else    if ("Intelliscore".equals(__fieldName)) {
      this.Intelliscore = (Double) __fieldVal;
    }
    else    if ("JobsRecentJobs".equals(__fieldName)) {
      this.JobsRecentJobs = (Double) __fieldVal;
    }
    else    if ("JobsTrendString".equals(__fieldName)) {
      this.JobsTrendString = (String) __fieldVal;
    }
    else    if ("ModelAction".equals(__fieldName)) {
      this.ModelAction = (String) __fieldVal;
    }
    else    if ("PercentileModel".equals(__fieldName)) {
      this.PercentileModel = (Double) __fieldVal;
    }
    else    if ("RetirementAssetsEOY".equals(__fieldName)) {
      this.RetirementAssetsEOY = (Double) __fieldVal;
    }
    else    if ("RetirementAssetsYOY".equals(__fieldName)) {
      this.RetirementAssetsYOY = (Double) __fieldVal;
    }
    else    if ("TotalParticipantsSOY".equals(__fieldName)) {
      this.TotalParticipantsSOY = (Double) __fieldVal;
    }
    else    if ("UCCFilings".equals(__fieldName)) {
      this.UCCFilings = (Double) __fieldVal;
    }
    else    if ("UCCFilingsPresent".equals(__fieldName)) {
      this.UCCFilingsPresent = (String) __fieldVal;
    }
    else    if ("Years_in_Business_Code".equals(__fieldName)) {
      this.Years_in_Business_Code = (String) __fieldVal;
    }
    else    if ("Non_Profit_Indicator".equals(__fieldName)) {
      this.Non_Profit_Indicator = (String) __fieldVal;
    }
    else    if ("PD_DA_AwardCategory".equals(__fieldName)) {
      this.PD_DA_AwardCategory = (String) __fieldVal;
    }
    else    if ("PD_DA_JobTitle".equals(__fieldName)) {
      this.PD_DA_JobTitle = (String) __fieldVal;
    }
    else    if ("PD_DA_LastSocialActivity_Units".equals(__fieldName)) {
      this.PD_DA_LastSocialActivity_Units = (String) __fieldVal;
    }
    else    if ("PD_DA_MonthsPatentGranted".equals(__fieldName)) {
      this.PD_DA_MonthsPatentGranted = (Double) __fieldVal;
    }
    else    if ("PD_DA_MonthsSinceFundAwardDate".equals(__fieldName)) {
      this.PD_DA_MonthsSinceFundAwardDate = (Double) __fieldVal;
    }
    else    if ("PD_DA_PrimarySIC1".equals(__fieldName)) {
      this.PD_DA_PrimarySIC1 = (String) __fieldVal;
    }
    else    if ("Industry".equals(__fieldName)) {
      this.Industry = (String) __fieldVal;
    }
    else    if ("LeadSource".equals(__fieldName)) {
      this.LeadSource = (String) __fieldVal;
    }
    else    if ("AnnualRevenue".equals(__fieldName)) {
      this.AnnualRevenue = (Double) __fieldVal;
    }
    else    if ("NumberOfEmployees".equals(__fieldName)) {
      this.NumberOfEmployees = (Double) __fieldVal;
    }
    else    if ("Alexa_MonthsSinceOnline".equals(__fieldName)) {
      this.Alexa_MonthsSinceOnline = (Double) __fieldVal;
    }
    else    if ("Alexa_Rank".equals(__fieldName)) {
      this.Alexa_Rank = (Double) __fieldVal;
    }
    else    if ("Alexa_ReachPerMillion".equals(__fieldName)) {
      this.Alexa_ReachPerMillion = (Double) __fieldVal;
    }
    else    if ("Alexa_ViewsPerMillion".equals(__fieldName)) {
      this.Alexa_ViewsPerMillion = (Double) __fieldVal;
    }
    else    if ("Alexa_ViewsPerUser".equals(__fieldName)) {
      this.Alexa_ViewsPerUser = (Double) __fieldVal;
    }
    else    if ("BW_TechTags_Cnt".equals(__fieldName)) {
      this.BW_TechTags_Cnt = (Double) __fieldVal;
    }
    else    if ("BW_TotalTech_Cnt".equals(__fieldName)) {
      this.BW_TotalTech_Cnt = (Double) __fieldVal;
    }
    else    if ("BW_ads".equals(__fieldName)) {
      this.BW_ads = (Double) __fieldVal;
    }
    else    if ("BW_analytics".equals(__fieldName)) {
      this.BW_analytics = (Double) __fieldVal;
    }
    else    if ("BW_cdn".equals(__fieldName)) {
      this.BW_cdn = (Double) __fieldVal;
    }
    else    if ("BW_cdns".equals(__fieldName)) {
      this.BW_cdns = (Double) __fieldVal;
    }
    else    if ("BW_cms".equals(__fieldName)) {
      this.BW_cms = (Double) __fieldVal;
    }
    else    if ("BW_docinfo".equals(__fieldName)) {
      this.BW_docinfo = (Double) __fieldVal;
    }
    else    if ("BW_encoding".equals(__fieldName)) {
      this.BW_encoding = (Double) __fieldVal;
    }
    else    if ("BW_feeds".equals(__fieldName)) {
      this.BW_feeds = (Double) __fieldVal;
    }
    else    if ("BW_framework".equals(__fieldName)) {
      this.BW_framework = (Double) __fieldVal;
    }
    else    if ("BW_hosting".equals(__fieldName)) {
      this.BW_hosting = (Double) __fieldVal;
    }
    else    if ("BW_javascript".equals(__fieldName)) {
      this.BW_javascript = (Double) __fieldVal;
    }
    else    if ("BW_mapping".equals(__fieldName)) {
      this.BW_mapping = (Double) __fieldVal;
    }
    else    if ("BW_media".equals(__fieldName)) {
      this.BW_media = (Double) __fieldVal;
    }
    else    if ("BW_mx".equals(__fieldName)) {
      this.BW_mx = (Double) __fieldVal;
    }
    else    if ("BW_ns".equals(__fieldName)) {
      this.BW_ns = (Double) __fieldVal;
    }
    else    if ("BW_parked".equals(__fieldName)) {
      this.BW_parked = (Double) __fieldVal;
    }
    else    if ("BW_payment".equals(__fieldName)) {
      this.BW_payment = (Double) __fieldVal;
    }
    else    if ("BW_seo_headers".equals(__fieldName)) {
      this.BW_seo_headers = (Double) __fieldVal;
    }
    else    if ("BW_seo_meta".equals(__fieldName)) {
      this.BW_seo_meta = (Double) __fieldVal;
    }
    else    if ("BW_seo_title".equals(__fieldName)) {
      this.BW_seo_title = (Double) __fieldVal;
    }
    else    if ("BW_Server".equals(__fieldName)) {
      this.BW_Server = (Double) __fieldVal;
    }
    else    if ("BW_shop".equals(__fieldName)) {
      this.BW_shop = (Double) __fieldVal;
    }
    else    if ("BW_ssl".equals(__fieldName)) {
      this.BW_ssl = (Double) __fieldVal;
    }
    else    if ("BW_Web_Master".equals(__fieldName)) {
      this.BW_Web_Master = (Double) __fieldVal;
    }
    else    if ("BW_Web_Server".equals(__fieldName)) {
      this.BW_Web_Server = (Double) __fieldVal;
    }
    else    if ("BW_widgets".equals(__fieldName)) {
      this.BW_widgets = (Double) __fieldVal;
    }
    else    if ("Activity_ClickLink_cnt".equals(__fieldName)) {
      this.Activity_ClickLink_cnt = (Double) __fieldVal;
    }
    else    if ("Activity_VisitWeb_cnt".equals(__fieldName)) {
      this.Activity_VisitWeb_cnt = (Double) __fieldVal;
    }
    else    if ("Activity_InterestingMoment_cnt".equals(__fieldName)) {
      this.Activity_InterestingMoment_cnt = (Double) __fieldVal;
    }
    else    if ("Activity_OpenEmail_cnt".equals(__fieldName)) {
      this.Activity_OpenEmail_cnt = (Double) __fieldVal;
    }
    else    if ("Activity_EmailBncedSft_cnt".equals(__fieldName)) {
      this.Activity_EmailBncedSft_cnt = (Double) __fieldVal;
    }
    else    if ("Activity_FillOutForm_cnt".equals(__fieldName)) {
      this.Activity_FillOutForm_cnt = (Double) __fieldVal;
    }
    else    if ("Activity_UnsubscrbEmail_cnt".equals(__fieldName)) {
      this.Activity_UnsubscrbEmail_cnt = (Double) __fieldVal;
    }
    else    if ("Activity_ClickEmail_cnt".equals(__fieldName)) {
      this.Activity_ClickEmail_cnt = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_CloudService_One".equals(__fieldName)) {
      this.CloudTechnologies_CloudService_One = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_CloudService_Two".equals(__fieldName)) {
      this.CloudTechnologies_CloudService_Two = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_CommTech_One".equals(__fieldName)) {
      this.CloudTechnologies_CommTech_One = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_CommTech_Two".equals(__fieldName)) {
      this.CloudTechnologies_CommTech_Two = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_CRM_One".equals(__fieldName)) {
      this.CloudTechnologies_CRM_One = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_CRM_Two".equals(__fieldName)) {
      this.CloudTechnologies_CRM_Two = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_DataCenterSolutions_One".equals(__fieldName)) {
      this.CloudTechnologies_DataCenterSolutions_One = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_DataCenterSolutions_Two".equals(__fieldName)) {
      this.CloudTechnologies_DataCenterSolutions_Two = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_EnterpriseApplications_One".equals(__fieldName)) {
      this.CloudTechnologies_EnterpriseApplications_One = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_EnterpriseApplications_Two".equals(__fieldName)) {
      this.CloudTechnologies_EnterpriseApplications_Two = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_EnterpriseContent_One".equals(__fieldName)) {
      this.CloudTechnologies_EnterpriseContent_One = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_EnterpriseContent_Two".equals(__fieldName)) {
      this.CloudTechnologies_EnterpriseContent_Two = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_HardwareBasic_One".equals(__fieldName)) {
      this.CloudTechnologies_HardwareBasic_One = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_HardwareBasic_Two".equals(__fieldName)) {
      this.CloudTechnologies_HardwareBasic_Two = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_ITGovernance_One".equals(__fieldName)) {
      this.CloudTechnologies_ITGovernance_One = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_ITGovernance_Two".equals(__fieldName)) {
      this.CloudTechnologies_ITGovernance_Two = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_MarketingPerfMgmt_One".equals(__fieldName)) {
      this.CloudTechnologies_MarketingPerfMgmt_One = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_MarketingPerfMgmt_Two".equals(__fieldName)) {
      this.CloudTechnologies_MarketingPerfMgmt_Two = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_NetworkComputing_One".equals(__fieldName)) {
      this.CloudTechnologies_NetworkComputing_One = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_NetworkComputing_Two".equals(__fieldName)) {
      this.CloudTechnologies_NetworkComputing_Two = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_ProductivitySltns_One".equals(__fieldName)) {
      this.CloudTechnologies_ProductivitySltns_One = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_ProductivitySltns_Two".equals(__fieldName)) {
      this.CloudTechnologies_ProductivitySltns_Two = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_ProjectMgnt_One".equals(__fieldName)) {
      this.CloudTechnologies_ProjectMgnt_One = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_ProjectMgnt_Two".equals(__fieldName)) {
      this.CloudTechnologies_ProjectMgnt_Two = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_SoftwareBasic_One".equals(__fieldName)) {
      this.CloudTechnologies_SoftwareBasic_One = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_SoftwareBasic_Two".equals(__fieldName)) {
      this.CloudTechnologies_SoftwareBasic_Two = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_VerticalMarkets_One".equals(__fieldName)) {
      this.CloudTechnologies_VerticalMarkets_One = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_VerticalMarkets_Two".equals(__fieldName)) {
      this.CloudTechnologies_VerticalMarkets_Two = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_WebOrntdArch_One".equals(__fieldName)) {
      this.CloudTechnologies_WebOrntdArch_One = (Double) __fieldVal;
    }
    else    if ("CloudTechnologies_WebOrntdArch_Two".equals(__fieldName)) {
      this.CloudTechnologies_WebOrntdArch_Two = (Double) __fieldVal;
    }
    else {
      throw new RuntimeException("No such field: " + __fieldName);
    }
  }
  public boolean setField0(String __fieldName, Object __fieldVal) {
    if ("Nutanix_EventTable_Clean".equals(__fieldName)) {
      this.Nutanix_EventTable_Clean = (Long) __fieldVal;
      return true;
    }
    else    if ("LeadID".equals(__fieldName)) {
      this.LeadID = (String) __fieldVal;
      return true;
    }
    else    if ("CustomerID".equals(__fieldName)) {
      this.CustomerID = (String) __fieldVal;
      return true;
    }
    else    if ("PeriodID".equals(__fieldName)) {
      this.PeriodID = (Long) __fieldVal;
      return true;
    }
    else    if ("P1_Target".equals(__fieldName)) {
      this.P1_Target = (Long) __fieldVal;
      return true;
    }
    else    if ("P1_TargetTraining".equals(__fieldName)) {
      this.P1_TargetTraining = (Long) __fieldVal;
      return true;
    }
    else    if ("P1_Event".equals(__fieldName)) {
      this.P1_Event = (String) __fieldVal;
      return true;
    }
    else    if ("Company".equals(__fieldName)) {
      this.Company = (String) __fieldVal;
      return true;
    }
    else    if ("Email".equals(__fieldName)) {
      this.Email = (String) __fieldVal;
      return true;
    }
    else    if ("Domain".equals(__fieldName)) {
      this.Domain = (String) __fieldVal;
      return true;
    }
    else    if ("AwardYear".equals(__fieldName)) {
      this.AwardYear = (Long) __fieldVal;
      return true;
    }
    else    if ("BankruptcyFiled".equals(__fieldName)) {
      this.BankruptcyFiled = (String) __fieldVal;
      return true;
    }
    else    if ("BusinessAnnualSalesAbs".equals(__fieldName)) {
      this.BusinessAnnualSalesAbs = (Double) __fieldVal;
      return true;
    }
    else    if ("BusinessAssets".equals(__fieldName)) {
      this.BusinessAssets = (String) __fieldVal;
      return true;
    }
    else    if ("BusinessECommerceSite".equals(__fieldName)) {
      this.BusinessECommerceSite = (String) __fieldVal;
      return true;
    }
    else    if ("BusinessEntityType".equals(__fieldName)) {
      this.BusinessEntityType = (String) __fieldVal;
      return true;
    }
    else    if ("BusinessEstablishedYear".equals(__fieldName)) {
      this.BusinessEstablishedYear = (Double) __fieldVal;
      return true;
    }
    else    if ("BusinessEstimatedAnnualSales_k".equals(__fieldName)) {
      this.BusinessEstimatedAnnualSales_k = (Double) __fieldVal;
      return true;
    }
    else    if ("BusinessEstimatedEmployees".equals(__fieldName)) {
      this.BusinessEstimatedEmployees = (Double) __fieldVal;
      return true;
    }
    else    if ("BusinessFirmographicsParentEmployees".equals(__fieldName)) {
      this.BusinessFirmographicsParentEmployees = (Double) __fieldVal;
      return true;
    }
    else    if ("BusinessFirmographicsParentRevenue".equals(__fieldName)) {
      this.BusinessFirmographicsParentRevenue = (Double) __fieldVal;
      return true;
    }
    else    if ("BusinessIndustrySector".equals(__fieldName)) {
      this.BusinessIndustrySector = (String) __fieldVal;
      return true;
    }
    else    if ("BusinessRetirementParticipants".equals(__fieldName)) {
      this.BusinessRetirementParticipants = (Double) __fieldVal;
      return true;
    }
    else    if ("BusinessSocialPresence".equals(__fieldName)) {
      this.BusinessSocialPresence = (String) __fieldVal;
      return true;
    }
    else    if ("BusinessUrlNumPages".equals(__fieldName)) {
      this.BusinessUrlNumPages = (Double) __fieldVal;
      return true;
    }
    else    if ("BusinessType".equals(__fieldName)) {
      this.BusinessType = (String) __fieldVal;
      return true;
    }
    else    if ("BusinessVCFunded".equals(__fieldName)) {
      this.BusinessVCFunded = (String) __fieldVal;
      return true;
    }
    else    if ("DerogatoryIndicator".equals(__fieldName)) {
      this.DerogatoryIndicator = (String) __fieldVal;
      return true;
    }
    else    if ("ExperianCreditRating".equals(__fieldName)) {
      this.ExperianCreditRating = (String) __fieldVal;
      return true;
    }
    else    if ("FundingAgency".equals(__fieldName)) {
      this.FundingAgency = (String) __fieldVal;
      return true;
    }
    else    if ("FundingAmount".equals(__fieldName)) {
      this.FundingAmount = (Double) __fieldVal;
      return true;
    }
    else    if ("FundingAwardAmount".equals(__fieldName)) {
      this.FundingAwardAmount = (Double) __fieldVal;
      return true;
    }
    else    if ("FundingFinanceRound".equals(__fieldName)) {
      this.FundingFinanceRound = (Double) __fieldVal;
      return true;
    }
    else    if ("FundingFiscalQuarter".equals(__fieldName)) {
      this.FundingFiscalQuarter = (Double) __fieldVal;
      return true;
    }
    else    if ("FundingFiscalYear".equals(__fieldName)) {
      this.FundingFiscalYear = (Double) __fieldVal;
      return true;
    }
    else    if ("FundingReceived".equals(__fieldName)) {
      this.FundingReceived = (Double) __fieldVal;
      return true;
    }
    else    if ("FundingStage".equals(__fieldName)) {
      this.FundingStage = (String) __fieldVal;
      return true;
    }
    else    if ("Intelliscore".equals(__fieldName)) {
      this.Intelliscore = (Double) __fieldVal;
      return true;
    }
    else    if ("JobsRecentJobs".equals(__fieldName)) {
      this.JobsRecentJobs = (Double) __fieldVal;
      return true;
    }
    else    if ("JobsTrendString".equals(__fieldName)) {
      this.JobsTrendString = (String) __fieldVal;
      return true;
    }
    else    if ("ModelAction".equals(__fieldName)) {
      this.ModelAction = (String) __fieldVal;
      return true;
    }
    else    if ("PercentileModel".equals(__fieldName)) {
      this.PercentileModel = (Double) __fieldVal;
      return true;
    }
    else    if ("RetirementAssetsEOY".equals(__fieldName)) {
      this.RetirementAssetsEOY = (Double) __fieldVal;
      return true;
    }
    else    if ("RetirementAssetsYOY".equals(__fieldName)) {
      this.RetirementAssetsYOY = (Double) __fieldVal;
      return true;
    }
    else    if ("TotalParticipantsSOY".equals(__fieldName)) {
      this.TotalParticipantsSOY = (Double) __fieldVal;
      return true;
    }
    else    if ("UCCFilings".equals(__fieldName)) {
      this.UCCFilings = (Double) __fieldVal;
      return true;
    }
    else    if ("UCCFilingsPresent".equals(__fieldName)) {
      this.UCCFilingsPresent = (String) __fieldVal;
      return true;
    }
    else    if ("Years_in_Business_Code".equals(__fieldName)) {
      this.Years_in_Business_Code = (String) __fieldVal;
      return true;
    }
    else    if ("Non_Profit_Indicator".equals(__fieldName)) {
      this.Non_Profit_Indicator = (String) __fieldVal;
      return true;
    }
    else    if ("PD_DA_AwardCategory".equals(__fieldName)) {
      this.PD_DA_AwardCategory = (String) __fieldVal;
      return true;
    }
    else    if ("PD_DA_JobTitle".equals(__fieldName)) {
      this.PD_DA_JobTitle = (String) __fieldVal;
      return true;
    }
    else    if ("PD_DA_LastSocialActivity_Units".equals(__fieldName)) {
      this.PD_DA_LastSocialActivity_Units = (String) __fieldVal;
      return true;
    }
    else    if ("PD_DA_MonthsPatentGranted".equals(__fieldName)) {
      this.PD_DA_MonthsPatentGranted = (Double) __fieldVal;
      return true;
    }
    else    if ("PD_DA_MonthsSinceFundAwardDate".equals(__fieldName)) {
      this.PD_DA_MonthsSinceFundAwardDate = (Double) __fieldVal;
      return true;
    }
    else    if ("PD_DA_PrimarySIC1".equals(__fieldName)) {
      this.PD_DA_PrimarySIC1 = (String) __fieldVal;
      return true;
    }
    else    if ("Industry".equals(__fieldName)) {
      this.Industry = (String) __fieldVal;
      return true;
    }
    else    if ("LeadSource".equals(__fieldName)) {
      this.LeadSource = (String) __fieldVal;
      return true;
    }
    else    if ("AnnualRevenue".equals(__fieldName)) {
      this.AnnualRevenue = (Double) __fieldVal;
      return true;
    }
    else    if ("NumberOfEmployees".equals(__fieldName)) {
      this.NumberOfEmployees = (Double) __fieldVal;
      return true;
    }
    else    if ("Alexa_MonthsSinceOnline".equals(__fieldName)) {
      this.Alexa_MonthsSinceOnline = (Double) __fieldVal;
      return true;
    }
    else    if ("Alexa_Rank".equals(__fieldName)) {
      this.Alexa_Rank = (Double) __fieldVal;
      return true;
    }
    else    if ("Alexa_ReachPerMillion".equals(__fieldName)) {
      this.Alexa_ReachPerMillion = (Double) __fieldVal;
      return true;
    }
    else    if ("Alexa_ViewsPerMillion".equals(__fieldName)) {
      this.Alexa_ViewsPerMillion = (Double) __fieldVal;
      return true;
    }
    else    if ("Alexa_ViewsPerUser".equals(__fieldName)) {
      this.Alexa_ViewsPerUser = (Double) __fieldVal;
      return true;
    }
    else    if ("BW_TechTags_Cnt".equals(__fieldName)) {
      this.BW_TechTags_Cnt = (Double) __fieldVal;
      return true;
    }
    else    if ("BW_TotalTech_Cnt".equals(__fieldName)) {
      this.BW_TotalTech_Cnt = (Double) __fieldVal;
      return true;
    }
    else    if ("BW_ads".equals(__fieldName)) {
      this.BW_ads = (Double) __fieldVal;
      return true;
    }
    else    if ("BW_analytics".equals(__fieldName)) {
      this.BW_analytics = (Double) __fieldVal;
      return true;
    }
    else    if ("BW_cdn".equals(__fieldName)) {
      this.BW_cdn = (Double) __fieldVal;
      return true;
    }
    else    if ("BW_cdns".equals(__fieldName)) {
      this.BW_cdns = (Double) __fieldVal;
      return true;
    }
    else    if ("BW_cms".equals(__fieldName)) {
      this.BW_cms = (Double) __fieldVal;
      return true;
    }
    else    if ("BW_docinfo".equals(__fieldName)) {
      this.BW_docinfo = (Double) __fieldVal;
      return true;
    }
    else    if ("BW_encoding".equals(__fieldName)) {
      this.BW_encoding = (Double) __fieldVal;
      return true;
    }
    else    if ("BW_feeds".equals(__fieldName)) {
      this.BW_feeds = (Double) __fieldVal;
      return true;
    }
    else    if ("BW_framework".equals(__fieldName)) {
      this.BW_framework = (Double) __fieldVal;
      return true;
    }
    else    if ("BW_hosting".equals(__fieldName)) {
      this.BW_hosting = (Double) __fieldVal;
      return true;
    }
    else    if ("BW_javascript".equals(__fieldName)) {
      this.BW_javascript = (Double) __fieldVal;
      return true;
    }
    else    if ("BW_mapping".equals(__fieldName)) {
      this.BW_mapping = (Double) __fieldVal;
      return true;
    }
    else    if ("BW_media".equals(__fieldName)) {
      this.BW_media = (Double) __fieldVal;
      return true;
    }
    else    if ("BW_mx".equals(__fieldName)) {
      this.BW_mx = (Double) __fieldVal;
      return true;
    }
    else    if ("BW_ns".equals(__fieldName)) {
      this.BW_ns = (Double) __fieldVal;
      return true;
    }
    else    if ("BW_parked".equals(__fieldName)) {
      this.BW_parked = (Double) __fieldVal;
      return true;
    }
    else    if ("BW_payment".equals(__fieldName)) {
      this.BW_payment = (Double) __fieldVal;
      return true;
    }
    else    if ("BW_seo_headers".equals(__fieldName)) {
      this.BW_seo_headers = (Double) __fieldVal;
      return true;
    }
    else    if ("BW_seo_meta".equals(__fieldName)) {
      this.BW_seo_meta = (Double) __fieldVal;
      return true;
    }
    else    if ("BW_seo_title".equals(__fieldName)) {
      this.BW_seo_title = (Double) __fieldVal;
      return true;
    }
    else    if ("BW_Server".equals(__fieldName)) {
      this.BW_Server = (Double) __fieldVal;
      return true;
    }
    else    if ("BW_shop".equals(__fieldName)) {
      this.BW_shop = (Double) __fieldVal;
      return true;
    }
    else    if ("BW_ssl".equals(__fieldName)) {
      this.BW_ssl = (Double) __fieldVal;
      return true;
    }
    else    if ("BW_Web_Master".equals(__fieldName)) {
      this.BW_Web_Master = (Double) __fieldVal;
      return true;
    }
    else    if ("BW_Web_Server".equals(__fieldName)) {
      this.BW_Web_Server = (Double) __fieldVal;
      return true;
    }
    else    if ("BW_widgets".equals(__fieldName)) {
      this.BW_widgets = (Double) __fieldVal;
      return true;
    }
    else    if ("Activity_ClickLink_cnt".equals(__fieldName)) {
      this.Activity_ClickLink_cnt = (Double) __fieldVal;
      return true;
    }
    else    if ("Activity_VisitWeb_cnt".equals(__fieldName)) {
      this.Activity_VisitWeb_cnt = (Double) __fieldVal;
      return true;
    }
    else    if ("Activity_InterestingMoment_cnt".equals(__fieldName)) {
      this.Activity_InterestingMoment_cnt = (Double) __fieldVal;
      return true;
    }
    else    if ("Activity_OpenEmail_cnt".equals(__fieldName)) {
      this.Activity_OpenEmail_cnt = (Double) __fieldVal;
      return true;
    }
    else    if ("Activity_EmailBncedSft_cnt".equals(__fieldName)) {
      this.Activity_EmailBncedSft_cnt = (Double) __fieldVal;
      return true;
    }
    else    if ("Activity_FillOutForm_cnt".equals(__fieldName)) {
      this.Activity_FillOutForm_cnt = (Double) __fieldVal;
      return true;
    }
    else    if ("Activity_UnsubscrbEmail_cnt".equals(__fieldName)) {
      this.Activity_UnsubscrbEmail_cnt = (Double) __fieldVal;
      return true;
    }
    else    if ("Activity_ClickEmail_cnt".equals(__fieldName)) {
      this.Activity_ClickEmail_cnt = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_CloudService_One".equals(__fieldName)) {
      this.CloudTechnologies_CloudService_One = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_CloudService_Two".equals(__fieldName)) {
      this.CloudTechnologies_CloudService_Two = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_CommTech_One".equals(__fieldName)) {
      this.CloudTechnologies_CommTech_One = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_CommTech_Two".equals(__fieldName)) {
      this.CloudTechnologies_CommTech_Two = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_CRM_One".equals(__fieldName)) {
      this.CloudTechnologies_CRM_One = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_CRM_Two".equals(__fieldName)) {
      this.CloudTechnologies_CRM_Two = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_DataCenterSolutions_One".equals(__fieldName)) {
      this.CloudTechnologies_DataCenterSolutions_One = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_DataCenterSolutions_Two".equals(__fieldName)) {
      this.CloudTechnologies_DataCenterSolutions_Two = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_EnterpriseApplications_One".equals(__fieldName)) {
      this.CloudTechnologies_EnterpriseApplications_One = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_EnterpriseApplications_Two".equals(__fieldName)) {
      this.CloudTechnologies_EnterpriseApplications_Two = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_EnterpriseContent_One".equals(__fieldName)) {
      this.CloudTechnologies_EnterpriseContent_One = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_EnterpriseContent_Two".equals(__fieldName)) {
      this.CloudTechnologies_EnterpriseContent_Two = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_HardwareBasic_One".equals(__fieldName)) {
      this.CloudTechnologies_HardwareBasic_One = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_HardwareBasic_Two".equals(__fieldName)) {
      this.CloudTechnologies_HardwareBasic_Two = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_ITGovernance_One".equals(__fieldName)) {
      this.CloudTechnologies_ITGovernance_One = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_ITGovernance_Two".equals(__fieldName)) {
      this.CloudTechnologies_ITGovernance_Two = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_MarketingPerfMgmt_One".equals(__fieldName)) {
      this.CloudTechnologies_MarketingPerfMgmt_One = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_MarketingPerfMgmt_Two".equals(__fieldName)) {
      this.CloudTechnologies_MarketingPerfMgmt_Two = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_NetworkComputing_One".equals(__fieldName)) {
      this.CloudTechnologies_NetworkComputing_One = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_NetworkComputing_Two".equals(__fieldName)) {
      this.CloudTechnologies_NetworkComputing_Two = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_ProductivitySltns_One".equals(__fieldName)) {
      this.CloudTechnologies_ProductivitySltns_One = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_ProductivitySltns_Two".equals(__fieldName)) {
      this.CloudTechnologies_ProductivitySltns_Two = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_ProjectMgnt_One".equals(__fieldName)) {
      this.CloudTechnologies_ProjectMgnt_One = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_ProjectMgnt_Two".equals(__fieldName)) {
      this.CloudTechnologies_ProjectMgnt_Two = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_SoftwareBasic_One".equals(__fieldName)) {
      this.CloudTechnologies_SoftwareBasic_One = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_SoftwareBasic_Two".equals(__fieldName)) {
      this.CloudTechnologies_SoftwareBasic_Two = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_VerticalMarkets_One".equals(__fieldName)) {
      this.CloudTechnologies_VerticalMarkets_One = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_VerticalMarkets_Two".equals(__fieldName)) {
      this.CloudTechnologies_VerticalMarkets_Two = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_WebOrntdArch_One".equals(__fieldName)) {
      this.CloudTechnologies_WebOrntdArch_One = (Double) __fieldVal;
      return true;
    }
    else    if ("CloudTechnologies_WebOrntdArch_Two".equals(__fieldName)) {
      this.CloudTechnologies_WebOrntdArch_Two = (Double) __fieldVal;
      return true;
    }
    else {
      return false;    }
  }
}
