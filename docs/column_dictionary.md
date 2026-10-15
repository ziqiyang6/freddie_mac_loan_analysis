# Freddie Mac SFLLD Release 47 — Column Dictionary

## Origination Data File — 31 Columns

| # | Column | 含义 |
|---|---|---|
| 1 | `classic_fico` | 借款人在贷款发放时使用的 Classic FICO 信用评分，用于衡量借款人的信用状况。 |
| 2 | `first_payment_date` | 贷款合同规定的第一笔月供到期月份。 |
| 3 | `first_time_homebuyer_indicator` | 表示借款人是否属于首次购房者。 |
| 4 | `maturity_date` | 按原始贷款合同计算的最后一笔月供到期月份，即贷款到期月份。 |
| 5 | `msa` | 抵押房产所在的 Metropolitan Statistical Area（MSA）或 Metropolitan Division。 |
| 6 | `mortgage_insurance_percentage` | Freddie Mac 收购贷款时，Mortgage Insurance 对贷款损失提供的保险覆盖比例。 |
| 7 | `number_of_units` | 抵押房产包含的住宅单元数量，例如 1-unit、2-unit 等。 |
| 8 | `occupancy_status` | 房产的使用性质，例如自住房、投资房或第二套住房。 |
| 9 | `original_cltv` | Original Combined Loan-to-Value，包含第一顺位贷款和其他已披露抵押贷款后的综合贷款价值比。 |
| 10 | `original_dti` | Original Debt-to-Income Ratio，贷款发放时借款人每月债务支出相对于月收入的比例。 |
| 11 | `original_upb` | Original Unpaid Principal Balance，贷款发放时的原始贷款本金余额。 |
| 12 | `original_ltv` | Original Loan-to-Value，原始贷款金额相对于抵押房产价值的比例。 |
| 13 | `original_interest_rate` | 贷款发放时合同约定的原始利率。 |
| 14 | `channel` | 贷款的发放渠道，例如 Retail、Broker 或 Correspondent。 |
| 15 | `prepayment_penalty_indicator` | 表示贷款是否包含提前还款罚金条款。 |
| 16 | `amortization_type` | 贷款的摊还类型，例如 Fixed-Rate Mortgage 或 Adjustable-Rate Mortgage。 |
| 17 | `property_state` | 抵押房产所在的州或美国领地。 |
| 18 | `property_type` | 抵押房产类型，例如 Single-Family、Condo、PUD、Manufactured Housing 等。 |
| 19 | `postal_code` | 抵押房产所在地区的邮编信息；公开数据仅保留部分邮编信息。 |
| 20 | `loan_identifier` | Freddie Mac 为每笔贷款分配的唯一 Loan Identifier，用于连接 Origination 和 Performance 数据。 |
| 21 | `loan_purpose` | 贷款用途，例如购房、Cash-Out Refinance 或 No Cash-Out Refinance。 |
| 22 | `original_loan_term` | 原始贷款期限，以计划还款月数表示，例如 360 个月。 |
| 23 | `number_of_borrowers` | 对该笔贷款承担还款责任的借款人数。 |
| 24 | `seller_name` | 将该笔贷款出售给 Freddie Mac 的 Seller 名称。 |
| 25 | `super_conforming_flag` | 表示贷款是否属于 Super Conforming Mortgage。 |
| 26 | `pre_harp_loan_sequence_number` | 对 HARP / Relief Refinance 贷款，用于关联再融资之前原贷款的 Loan Identifier。 |
| 27 | `special_eligibility_program` | 表示贷款是否属于 Freddie Mac 的特殊 eligibility program，例如 Home Possible、HFA Advantage 或 Refi Possible。 |
| 28 | `harp_indicator` | 表示贷款是否属于 HARP / Relief Refinance 类型贷款。 |
| 29 | `property_valuation_method` | 贷款发放时用于确定房产价值的方法，例如 Appraisal、Appraisal Waiver 或 ACE+PDR。 |
| 30 | `interest_only_indicator` | 表示贷款是否存在只支付利息、不偿还本金的 Interest-Only 阶段。 |
| 31 | `vantagescore_4_0` | 借款人在贷款发放过程中使用的 VantageScore 4.0 信用评分。 |

---

## Monthly Performance Data File — 35 Columns

| # | Column | 含义 |
|---|---|---|
| 1 | `loan_identifier` | 每笔贷款的唯一标识，用于与 Origination 文件中的贷款关联。 |
| 2 | `period` | 当前这条 Performance Record 对应的月度报告期间。 |
| 3 | `current_actual_upb` | 当前月份贷款实际剩余的 Unpaid Principal Balance。 |
| 4 | `current_loan_delinquency_status` | 当前贷款逾期状态，表示贷款当前逾期了多少个月；也可以表示 REO 等特殊状态。 |
| 5 | `loan_age` | 截至当前报告月份，贷款已经经过的计划还款月数。 |
| 6 | `remaining_months_to_legal_maturity` | 当前月份距离贷款法律到期日还剩多少个月。 |
| 7 | `defect_settlement_date` | Freddie Mac 对已确认的 Underwriting Defect 或 Major Servicing Defect 完成处置或结算的月份。 |
| 8 | `modification_flag` | 表示该笔贷款是否在当前月份或之前发生过 Loan Modification。 |
| 9 | `zero_balance_code` | 当贷款余额变为 0 时，说明贷款退出数据集的原因，例如提前还清、Short Sale、Charge-Off 或 REO Disposition。 |
| 10 | `zero_balance_effective_date` | Zero Balance Code 对应的贷款终止事件实际生效月份。 |
| 11 | `current_interest_rate` | 当前月份贷款实际适用的利率，可能因 Loan Modification 而与原始利率不同。 |
| 12 | `current_non_interest_bearing_upb` | 当前剩余本金中不再计息的部分，通常与 Modification、Forbearance 或 Payment Deferral 有关。 |
| 13 | `ddlpi` | Due Date of Last Paid Installment，借款人最近一次已经完成支付的计划还款所对应的到期月份。 |
| 14 | `mi_recoveries` | Freddie Mac 因 Mortgage Insurance 获得的损失补偿金额。 |
| 15 | `net_sale_proceeds` | 房产处置或贷款出售后 Freddie Mac 获得的净销售收入。 |
| 16 | `non_mi_recoveries` | 除 Mortgage Insurance 外，Freddie Mac 从其他渠道获得的损失补偿或回收金额。 |
| 17 | `total_expenses` | Freddie Mac 因贷款违约、房产维护和处置等产生的总费用。 |
| 18 | `legal_costs` | 与违约处理、foreclosure 或房产处置相关的法律费用。 |
| 19 | `maintenance_preservation_costs` | Freddie Mac 为维护、保护和管理抵押房产产生的费用。 |
| 20 | `taxes_and_insurance` | Freddie Mac 在房产处置过程中承担的税费和保险费用。 |
| 21 | `miscellaneous_expenses` | 无法归入其他费用类别的其他贷款处置相关费用。 |
| 22 | `actual_loss` | Freddie Mac 在该笔贷款上最终确认的实际经济损失；负值代表经济收益。 |
| 23 | `cumulative_modification_costs` | 自贷款首次 Modification 以来累计产生的 Modification 成本。 |
| 24 | `interest_rate_step_indicator` | 表示最近一次 Loan Modification 是否包含分阶段变化的 Step Interest Rate。 |
| 25 | `payment_deferral_flag` | 表示贷款当前或以前是否使用过 Payment Deferral。 |
| 26 | `estimated_ltv` | 根据 Freddie Mac 对当前房产价值的估算计算出的 Current Estimated LTV。 |
| 27 | `zero_balance_removal_upb` | 在应用 Zero Balance Code、贷款余额归零之前的剩余贷款本金。 |
| 28 | `delinquent_accrued_interest` | 贷款发生严重违约或终止事件时已经累积但尚未支付的逾期利息。 |
| 29 | `delinquency_due_to_disaster` | 表示当前贷款逾期是否被 Servicer 报告为灾害相关 hardship 导致。 |
| 30 | `borrower_assistance_plan` | 借款人当前参加的还款援助计划，例如 Forbearance、Repayment Plan 或 Trial Period Plan。 |
| 31 | `current_period_modification_costs` | 当前报告月份因 Loan Modification、Principal Forbearance 或 Payment Deferral 产生的成本。 |
| 32 | `current_interest_bearing_upb` | 当前贷款余额中仍然需要计息的本金部分。 |
| 33 | `mortgage_insurance_cancellation_indicator` | 表示贷款的 Mortgage Insurance 是否已经取消。 |
| 34 | `servicer_name` | 当前负责该笔贷款 servicing 的 Servicer 名称。 |
| 35 | `bankruptcy_cramdown_costs` | 因借款人破产程序中的法院裁决导致本金减少等产生的 Bankruptcy Cramdown 相关成本。 |

## Term Explanation
Term	|   全称                         |	简单理解               |	风险含义
FICO    |	Credit Score                |   借款人的信用评分	    |   越低通常信用风险越高
DTI     |	Debt-to-Income Ratio	    |   每月债务支出 / 收入	    |   越高说明偿债压力越大
LTV	    |   Loan-to-Value	            |   贷款金额 / 房屋价值	    |   越高说明首付越少、equity 越低
CLTV	|   Combined LTV	            |   所有房贷 / 房屋价值	    |包含 second mortgage 等，总杠杆
UPB	    |   Unpaid Principal Balance	|   还没还掉的本金	        |表示当前 dollar exposure
DPD	    |   Days Past Due	            |   逾期多少天	           |最核心的 mortgage performance 指标之一
MI	    |   Mortgage Insurance	        |   房贷保险	           |高 LTV loan 常见，可降低 lender loss
Vintage	|   Origination Vintage	        |   同一时期发放的一批贷款	 |用于比较不同 cohort 的表现
