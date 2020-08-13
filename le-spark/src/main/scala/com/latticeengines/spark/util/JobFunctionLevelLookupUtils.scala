package com.latticeengines.spark.util

import org.apache.commons.lang3.StringUtils

private[spark] object JobFunctionLevelLookupUtils {
  private val funcCatagories = Map(
    "Accounting" -> List("Accounting", "Audit"),
    "Administrative" -> List("Admin", "Administrator", "Secretary",
      "Assistant", "Administrative", "Associate", "Office Manager"),
    "Arts and Design" -> List("Creative", "Writer/Editor", "Designer"),
    "Business Development" -> List("Principal", "President", "CEO", "VP", "Director", "Board Member",
      "Partner", "Chair", "General Business", "Board"),
    "Community & Social Services" -> List("Public Service", "Religious Leader", "Community Development"),
    "Consulting" -> List("Consultant", "Advisor", "Lobbyists", "Advisory Board"),
    "Education" -> List("Educator", "Student"),
    "Engineering" -> List("Engineering", "Network Administration & Engineering", "Systems Administration & Engineering",
      "Database Administration & Design"),
    "Entrepreneurship" -> List("Owner", "Founder"),
    "Finance" -> List("General Finance", "Finance Management", "Treasurer", "CFO", "Financial Analyst",
      "Financial Advisor", "Tax Specialists", "Payroll", "Corporate Finance and M&A", "Economist"),
    "Healthcare" -> List("Healthcare Professionals", "Healthcare"),
    "Human Resources" -> List("Employee Development", "General HR", "Recruiting", "HR Management", "Benefits and Comp"),
    "Information Technology" -> List("Information Technology", "Software Engineering", "Web Development & Management",
      "CTO", "Data Management", "CIO", "Computing & IT", "Information Security", "Records Management"),
    "Legal" -> List("Legal", "Compliance & Governance"),
    "Marketing" -> List("Marketing Management", "Marcomm", "General Marketing", "Advertising", "CMO", "Marketing"),
    "Media and Communications" -> List("Investor Relations"),
    "Military & Protective Services" -> List("Safety", "Veteran", "Solider"),
    "Operations" -> List("Operations", "COO", "Operations Management", "Logistics", "General Operations", "Maintenance",
      "Events", "Production", "Planner"),
    "Product Management" -> List("Product/Market Management"),
    "Program & Project Management" -> List("Unspecified Management", "Manager", "Project Management",
      "Analyst", "General Management"),
    "Purchasing" -> List("Purchasing", "Procurement"),
    "Quality Assurance" -> List("Quality Assurance", "QA"),
    "Real Estate" -> List("Technical/Construction", "Real Estate"),
    "Research" -> List("General R&D", "Research", "Research & Development"),
    "Sales" -> List("Sales", "Sales Executives", "Sales Support"),
    "Support" -> List("Support", "Customer Service", "Technical Support")
  )

  private val jobLevelPatterns = Map(
    "Partner" -> List("Partner", "Chairman", "President", "Founder"),
    "Owner" -> List("Owner", "Proprieter"),
    "CXO" -> List("CXO", "CEO", "CPO", "CMO", "CIO", "CTO", "CSO", "COO", "executive", "chief exec", "chief executive",
      "chief executive officer", "chief of staff", "chief product officer",
      "chief marketing officer", "chief information officer",
      "chief technology officer", "chief security officer", "chief operating officer"),
    "VP" -> List("Vice president", "vice pres", "v pres", "v president", "VP"),
    "Director" -> List("Director", "dir"),
    "Manager" -> List("Manager", "mgr", "analyst"),
    "Senior" -> List("Senior", "sr", "principal", "prin", "Staff", "supervisor", "leader", "advisor"),
    "Entry" -> List("associate", "consultant", "level")
  )

  private val jobFuncLookup = collection.mutable.Map[String, String]()

  private val jobLevelLookup = collection.mutable.Map[String, String]()

  for ((k, v) <- funcCatagories) {
    v.foreach { func =>
      jobFuncLookup.put(func.toLowerCase, k)
    }
  }

  for ((k, v) <- jobLevelPatterns) {
    v.foreach { level =>
      jobLevelLookup.put(level.toLowerCase, k)
    }
  }

  def getStandardJobFunction(func: String): String = {
    val standardFunc = if (func == null) "Other" else jobFuncLookup.getOrElse(func.toLowerCase, "Other")
    standardFunc
  }

  def getLevelFromTitleAndFunction(title: String, func: String): String = {
    val set = scala.collection.mutable.LinkedHashSet[String]()
    var level = "Other"
    // Split the title/function words and merge them into one word set
    if (StringUtils.isNotBlank(title)) {
      set += title
      set ++= title.toLowerCase.split("[/&,-.\\s]")
      level = "Entry"
    }
    if (StringUtils.isNotBlank(func)) {
      set += func
      set ++= func.toLowerCase.split("[/&,-.\\s]")
    }
    set.foreach { keyword =>
      if (jobLevelLookup.get(keyword) != None) {
        level = jobLevelLookup(keyword)
      }
    }
    level
  }
}
