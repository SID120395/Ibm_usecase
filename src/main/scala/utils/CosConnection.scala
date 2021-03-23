package utils

object CosConnection {
  /*
  driver code to perform KPI on CSV data and stored in different platform.
  */
  def main(args: Array[String]): Unit = {
    /*  Read the data from the given path  */
    val read_data = read_csv()
    /*  Creating KPI's with the given requirements  */
    val average_dpt_salary = avg_dept_sal(read_data)
    val gender_salary_gap = dept_sal_diff(read_data)
    val dept_gender_ratio = dept_gen_ratio(read_data)
    read_data.show(5)
    average_dpt_salary.show(5)
    gender_salary_gap.show(5)
    dept_gender_ratio.show(5)

  }
}
