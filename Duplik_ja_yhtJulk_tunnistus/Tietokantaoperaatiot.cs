using System;
using System.Data.SqlClient;
using System.Data;

namespace Duplik_ja_yhtJulk_tunnistus
{

    class SqlCon
    {
        public SqlConnection conn;
        public SqlCommand cmd;

        public SqlCon(string connString)
        {
            conn = new SqlConnection(connString);
            cmd = new SqlCommand();
            cmd.CommandType = CommandType.Text;
            cmd.CommandTimeout = 180;
            cmd.Connection = conn;
        }

        public void Avaa()
        {
            conn.Open();
        }

        public void Sulje()
        {
            cmd.Parameters.Clear();
            conn.Close();
        }
    }


    class Tietokantaoperaatiot
    {

        private SqlCon SqlConn;

        public Tietokantaoperaatiot() { }


        public Tietokantaoperaatiot(string connString)
        {
            SqlConn = new SqlCon(connString);
        }


        public int Hae_tarkistusID(string koodi)
        {
            SqlConn.Avaa();
            SqlConn.cmd.CommandText = "SELECT TarkistusID FROM Julkaisut_ods.dbo.Tarkistukset WHERE Koodi = @koodi";
            SqlConn.cmd.Parameters.AddWithValue("@koodi", koodi);

            int id = (int)Int32.Parse(SqlConn.cmd.ExecuteScalar().ToString());

            SqlConn.Sulje();

            return id;
        }


        public void Poista_poistetetut_julkaisut()
        {
            SqlConn.Avaa();

            // Tilakoodi tyhjä
            SqlConn.cmd.CommandText = @"
                DELETE o FROM [julkaisut_ods].[dbo].[ODS_JulkaisutTMP] o
                WHERE NOT EXISTS (select 1 from julkaisut_ods.dbo.ODS_Julkaisut where JulkaisunTunnus = o.JulkaisunTunnus) OR o.JulkaisunTilaKoodi is null";
            SqlConn.cmd.ExecuteNonQuery();

            // Poistetut
            SqlConn.cmd.CommandText = @"
                DELETE o FROM [julkaisut_ods].[dbo].[ODS_JulkaisutTMP] o
                WHERE EXISTS (select 1 from julkaisut_ods.dbo.ODS_Julkaisut where JulkaisunTunnus = o.JulkaisunTunnus and JulkaisunTilaKoodi = -1)";
            SqlConn.cmd.ExecuteNonQuery();

            // SA-taulussa olevat
            SqlConn.cmd.CommandText = @"
                DELETE o FROM [julkaisut_ods].[dbo].[ODS_JulkaisutTMP] o
                WHERE EXISTS (select 1 from julkaisut_ods.dbo.SA_Julkaisut where JulkaisunTunnus = o.JulkaisunTunnus)";
            SqlConn.cmd.ExecuteNonQuery();

            SqlConn.Sulje();
        }


        public void Tyhjenna_taulu(string table)
        {
            SqlConn.Avaa();
            SqlConn.cmd.CommandText = "TRUNCATE TABLE " + table;
            SqlConn.cmd.ExecuteNonQuery();
            SqlConn.Sulje();
        }


        public DataTable Lue_tietokantataulu_datatauluun(String ymp)
        {
            string taulu = "";
            if (ymp.ToLower() == "sa")
            {
                taulu = @"
                    FROM julkaisut_ods.dbo.SA_Julkaisut t1
                    --LEFT JOIN julkaisut_ods.dbo.EiDuplikaatti as t2 on t2.JulkaisunTunnus = t1.JulkaisunTunnus
                    WHERE 1=1 --t2.JulkaisunTunnus is null
                    and t1.JulkaisuVuosi >= @min_vuosi
                    and t1.JulkaisunTilaKoodi >= @tilaKoodi";
            }
            else if (ymp.ToLower() == "ods")
            {
                taulu = @"                  
                    FROM julkaisut_ods.dbo.ODS_Julkaisut t1 
                    LEFT JOIN julkaisut_ods.dbo.SA_Julkaisut s ON s.JulkaisunTunnus = t1.JulkaisunTunnus 
                    LEFT JOIN julkaisut_ods.dbo.ODS_JulkaisutTMP o2 on o2.JulkaisunTunnus = t1.JulkaisunTunnus
                    WHERE t1.JulkaisuVuosi >= @min_vuosi 
                    and t1.JulkaisunTilaKoodi between @tilaKoodi and 8
                    and s.JulkaisunTunnus is null
                    and o2.JulkaisunTunnus is null";
            }

            string kysely = @"
                    SELECT 
	                     t1.JulkaisunTunnus
                        ,t1.OrganisaatioTunnus
                        ,t1.JulkaisunOrgTunnus
                        ,t1.JulkaisutyyppiKoodi
                        ,t1.JulkaisuVuosi
                        ,t1.JulkaisunNimi
                        ,t1.KustantajanNimi
                        ,t1.EmojulkaisunNimi
                        ,t1.LehdenNimi
                        ,t1.DOI
                        ,t1.VolyymiTeksti
                        ,t1.LehdenNumeroTeksti
                        ,t1.SivunumeroTeksti
                        ,t1.Lataus_ID
                        ,t1.JulkaisunTilaKoodi
                        ,t1.AVSovellusTyyppiKoodi"
                    + taulu;

            SqlConn.cmd.CommandText = kysely;
            SqlConn.cmd.Parameters.AddWithValue("@min_vuosi", Globals.min_vuosi);
            SqlConn.cmd.Parameters.AddWithValue("@tilaKoodi", Globals.tilaKoodi_vertailtava_julkaisu);

            DataTable dt = new DataTable();
            SqlDataAdapter sda = new SqlDataAdapter(SqlConn.cmd);
            sda.Fill(dt);

            SqlConn.Sulje();

            return dt;
        }


        public void Kirjoita_datataulu_tietokantaan(DataTable dt, String taulu)
        {
            SqlConn.Avaa();

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(SqlConn.conn))
            {
                // Sarakkeiden mappaus datataulun ja tietokantataulun välillä
                foreach (DataColumn column in dt.Columns)
                {
                    // tätä voi käyttää debuggaukseen (K:\APP\log\stdout.txt), jos tulee virhettä (K:\APP\log\sterror.txt)
                    // ettei uuden sarakkeen jälkeen mappaus onnistu ja bulkCopy antaa virheen 
                    // Console.WriteLine($"Mapping: {column.ColumnName} -> {taulu}.{column.ColumnName}");
                    bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                }

                bulkCopy.BatchSize = 10000;
                bulkCopy.DestinationTableName = taulu;

                try
                {
                    bulkCopy.WriteToServer(dt);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    throw;
                }
            }

            SqlConn.Sulje();
        }


        public void Uudelleenjarjesta_indeksit(string taulu)
        {
            SqlConn.Avaa();
            SqlConn.cmd.CommandText = "ALTER INDEX ALL ON " + taulu + " REORGANIZE";
            SqlConn.cmd.ExecuteNonQuery();
            SqlConn.Sulje();
        }


        public void Uudelleenrakenna_indeksit(string taulu)
        {
            SqlConn.Avaa();
            SqlConn.cmd.CommandText = "ALTER INDEX ALL ON " + taulu + " REBUILD";
            SqlConn.cmd.ExecuteNonQuery();
            SqlConn.Sulje();
        }


        public void Esta_indeksit(string kanta, string skeema, string taulu)
        {
            SqlConn.Avaa();
            SqlConn.cmd.CommandText = "exec " + kanta + ".dbo.disable_non_clustered_indexes " + skeema + "," + taulu;
            SqlConn.cmd.ExecuteNonQuery();
            SqlConn.Sulje();
        }


        public void Paivita_ISSN_ja_ISBN_tunnukset(string taulu)
        {
            SqlConn.Avaa();

            // ISSN
            SqlConn.cmd.CommandText = @"
                UPDATE j
                SET
                     j.ISSN1 = d2.[1]
                    ,j.ISSN2 = d2.[2]
                FROM " + taulu + @" j
                LEFT JOIN (
                    SELECT
                        JulkaisunTunnus,[1],[2]
                    FROM (
                        SELECT
                            JulkaisunTunnus
                            , ISSN = nullif(ltrim(ISSN), '')
                            , rn = row_number() over(partition by JulkaisunTunnus order by Lataus_ID)
                        FROM [julkaisut_ods].[dbo].[SA_ISSN]
	                ) Q
                    PIVOT(
                        min(ISSN) FOR rn in ([1],[2])
	                ) pvt
                ) d2 on d2.JulkaisunTunnus = j.JulkaisunTunnus
            ";
            SqlConn.cmd.ExecuteNonQuery();

            // ISBN
            SqlConn.cmd.CommandText = @"
                UPDATE j
                SET
                     j.ISBN1 = d2.[1]
                    ,j.ISBN2 = d2.[2]
                FROM " + taulu + @" j
                LEFT JOIN (
                    SELECT
                        JulkaisunTunnus,[1],[2]
                    FROM (
                        SELECT
                            JulkaisunTunnus
                            , ISBN = nullif(ltrim(ISBN), '')
                            , rn = row_number() over(partition by JulkaisunTunnus order by Lataus_ID)
                        FROM [julkaisut_ods].[dbo].[SA_ISBN]
	                ) Q
                    PIVOT(
                        min(ISBN) FOR rn in ([1],[2])
	                ) pvt
                ) d2 on d2.JulkaisunTunnus = j.JulkaisunTunnus
            ";
            SqlConn.cmd.ExecuteNonQuery();

            SqlConn.Sulje();
        }


        public void Etsi_yhteisjulkaisut(int ehto)
        {
            SqlConn.Avaa();

            string sort_column = "JulkaisunTunnus";

            // Duplikaatin/yhteisjulkaisun etsinnässä priorisoidaan julkaisuja, jotka eivät ole SA_julkaisutTMP-taulussa eli samassa satsissa
            string with_columns = @"
                t1.dupl_JulkaisunTunnus
                ,t1.dupl_JulkaisunOrgTunnus
                ,t1.dupl_OrganisaatioTunnus
                --,t1.dupl_JulkaisutyyppiKoodi
                --,t1.dupl_JulkaisunNimi
                --,t1.dupl_DOI
                ,t1.dupl_yhtjulk_ehto
                ,t2.JulkaisunTunnus
                ,t2.JulkaisunOrgTunnus
                ,t2.OrganisaatioTunnus
                ,t2.JulkaisutyyppiKoodi
                ,t2.JulkaisunNimi
                ,t2.DOI
                ,rn = ROW_NUMBER() OVER (PARTITION BY t1.julkaisuntunnus ORDER BY (CASE WHEN t3.JulkaisunTunnus is null THEN 0 ELSE 1 END), t2." + sort_column + " desc )";

            string update_columns = @"
                t1.dupl_JulkaisunTunnus = JulkaisunTunnus
                ,t1.dupl_JulkaisunOrgTunnus = JulkaisunOrgTunnus
                ,t1.dupl_OrganisaatioTunnus = OrganisaatioTunnus
                --,t1.dupl_JulkaisutyyppiKoodi = JulkaisutyyppiKoodi
                --,t1.dupl_JulkaisunNimi = JulkaisunNimi
                --,t1.dupl_DOI = DOI
                ,t1.dupl_yhtjulk_ehto = " + ehto.ToString() + " ";

            // Tunnukset, joita ollaan poistamassa eikä siten hyväksytä duplikaattijulkaisuiksi
            //string ehto_1 = " and not exists(SELECT COUNT(*) FROM julkaisut_ods.dbo.SA_Julkaisut sa WHERE sa.JulkaisunTunnus = t2.JulkaisunTunnus AND sa.JulkaisunTilaKoodi = -1";
            string where_1_5 = "";
            string where_2_5 = "";

            if (ehto <= 9)
            {
                where_1_5 = @"
                    and not exists (
                        select 1
                        from julkaisut_ods.dbo.EiYhteisjulkaisuJulkaisutyyppiparit edjtp 
                        where (edjtp.JulkaisuTyyppi1 = t1.JulkaisutyyppiKoodi and edjtp.JulkaisuTyyppi2 = t2.JulkaisutyyppiKoodi) 
                        or (edjtp.JulkaisuTyyppi1 = t2.JulkaisutyyppiKoodi and edjtp.JulkaisuTyyppi2 = t1.JulkaisutyyppiKoodi)
                    ) ";
            }


            if (ehto > 1)
            {
                where_2_5 = "and (nullif(t1.DOI,'') is null or nullif(t2.DOI,'') is null or t1.DOI = t2.DOI) ";
            }


            string where_1_7_1 = "t1.dupl_yhtjulk is null";

            string where_1_7_2 = @" 
                and not exists (
                    select 1 
                    from julkaisut_ods.dbo.EiDuplikaattiTarkistusta edt 
                    where edt.organisaatiotunnus = t1.OrganisaatioTunnus 
                    and (
                        (edt.ekajulkorgtunnus = t1.JulkaisunOrgTunnus and edt.tokajulkorgtunnus = t2.JulkaisunOrgTunnus) 
                        or (edt.ekajulkorgtunnus = t2.JulkaisunOrgTunnus and edt.tokajulkorgtunnus = t1.JulkaisunOrgTunnus)
                    )
                ) ";

            // Tämä ei enää käytössä
            //string where_1_7_3 = @" 
            //    and not exists (
            //        select id 
            //        from julkaisut_ods.dbo.EiYhteisjulkaisuTarkistusta eyt 
            //        where (eyt.ekajulkaisuntunnus = t1.JulkaisunTunnus and eyt.tokajulkaisuntunnus = t2.JulkaisunTunnus) 
            //        or (eyt.ekajulkaisuntunnus = t2.JulkaisunTunnus and eyt.tokajulkaisuntunnus = t1.JulkaisunTunnus)
            //    ) ";

            string with_where = where_1_7_1 + where_1_7_2 + where_1_5 + where_2_5;

            string update_where = "t1.rn = 1";


            if (ehto == 1)
            {
                // Tunnistussääntö 1:        
                SqlConn.cmd.CommandText = @"
                WITH t1 AS
                (
                    SELECT " +
                        with_columns +
                    @"FROM julkaisut_ods.dbo.SA_JulkaisutTMP t1 
                    INNER JOIN julkaisut_ods.dbo.ODS_JulkaisutTMP t2 ON t2.DOI = t1.DOI and t2.JulkaisunTunnus != t1.JulkaisunTunnus and t1.DOI is not null
                    LEFT JOIN julkaisut_ods.dbo.SA_JulkaisutTMP t3 ON t3.JulkaisunTunnus = t2.JulkaisunTunnus
                    WHERE " + with_where +
                    @"and not ((t1.JulkaisutyyppiKoodi in ('A3', 'A4', 'B2', 'B3', 'D2', 'D3', 'E1') or t2.JulkaisutyyppiKoodi in ('A3', 'A4', 'B2', 'B3', 'D2', 'D3', 'E1')) and t1.JulkaisunNimi != t2.JulkaisunNimi)   
                ) 
                UPDATE t1
                SET " +
                update_columns +
                "WHERE " + update_where;

                SqlConn.cmd.ExecuteNonQuery();
            }

            else if (ehto == 2)
            {
                // Tunnistussääntö 2 v.1:
                SqlConn.cmd.CommandText = @"
                SELECT
		            i.ISSN,i.JulkaisunTunnus,t.JulkaisunNimi,t.SivunumeroTeksti,t.VolyymiTeksti,t.LehdenNumeroTeksti,t.JulkaisutyyppiKoodi,t.JulkaisunOrgTunnus,t.JulkaisunTilaKoodi,t.DOI,t.OrganisaatioTunnus
                INTO #temp
                FROM julkaisut_ods.dbo.ODS_ISSN i	
                INNER JOIN julkaisut_ods.dbo.ODS_JulkaisutTMP t on t.JulkaisunTunnus=i.JulkaisunTunnus
                WHERE EXISTS (select 1 from julkaisut_ods.dbo.SA_JulkaisutTMP WHERE ISSN1 = i.ISSN);

                WITH t1 AS
                (
                    SELECT " +
                        with_columns +
                    @"FROM julkaisut_ods.dbo.SA_JulkaisutTMP t1 
                    INNER JOIN #temp t2 on t2.ISSN = t1.ISSN1 
                        AND t2.JulkaisunTunnus != t1.JulkaisunTunnus 
                        AND t2.JulkaisunNimi = t1.JulkaisunNimi 
                        AND t2.VolyymiTeksti = t1.VolyymiTeksti 
                        AND t2.LehdenNumeroTeksti = t1.LehdenNumeroTeksti 
                        AND t2.SivunumeroTeksti = t1.SivunumeroTeksti 
                    LEFT JOIN julkaisut_ods.dbo.SA_JulkaisutTMP t3 ON t3.JulkaisunTunnus = t2.JulkaisunTunnus
                    WHERE " + with_where +

                @")
                UPDATE t1
                SET " +
                update_columns +
                "WHERE " + update_where +
                "DROP TABLE #temp";

                SqlConn.cmd.ExecuteNonQuery();
            }

            else if (ehto == 3)
            {
                // Tunnistussääntö 2 v.2:         
                SqlConn.cmd.CommandText = @"
                SELECT
		            i.ISSN,i.JulkaisunTunnus,t.JulkaisunNimi,t.SivunumeroTeksti,t.VolyymiTeksti,t.LehdenNumeroTeksti,t.JulkaisutyyppiKoodi,t.JulkaisunOrgTunnus,t.JulkaisunTilaKoodi,t.DOI,t.OrganisaatioTunnus
                INTO #temp
                FROM julkaisut_ods.dbo.ODS_ISSN i	
                INNER JOIN julkaisut_ods.dbo.ODS_JulkaisutTMP t on t.JulkaisunTunnus=i.JulkaisunTunnus
                WHERE EXISTS (select 1 from julkaisut_ods.dbo.SA_JulkaisutTMP WHERE ISSN2 = i.ISSN)

                ;WITH t1 AS
                (
                    SELECT " +
                        with_columns +
                    @"FROM julkaisut_ods.dbo.SA_JulkaisutTMP t1 
                    INNER JOIN #temp t2 on t2.ISSN = t1.ISSN2 
                        AND t2.JulkaisunTunnus != t1.JulkaisunTunnus 
                        AND t2.JulkaisunNimi = t1.JulkaisunNimi 
                        AND t2.VolyymiTeksti = t1.VolyymiTeksti 
                        AND t2.LehdenNumeroTeksti = t1.LehdenNumeroTeksti 
                        AND t2.SivunumeroTeksti = t1.SivunumeroTeksti 
                    LEFT JOIN julkaisut_ods.dbo.SA_JulkaisutTMP t3 ON t3.JulkaisunTunnus = t2.JulkaisunTunnus
                    WHERE " + with_where +

                @")
                UPDATE t1
                SET " +
                update_columns +
                "WHERE " + update_where +
                "DROP TABLE #temp";

                SqlConn.cmd.ExecuteNonQuery();
            }

            else if (ehto == 6)
            {
                // Tunnistussääntö 3:         
                SqlConn.cmd.CommandText = @"
                WITH t1 AS
                (
                    SELECT " +
                        with_columns +
                    @"FROM julkaisut_ods.dbo.SA_JulkaisutTMP t1 
                    INNER JOIN julkaisut_ods.dbo.ODS_JulkaisutTMP t2 ON 
                        t2.JulkaisutyyppiKoodi = t1.JulkaisutyyppiKoodi 
                        AND t2.JulkaisunNimi = t1.JulkaisunNimi 
                        AND t2.KustantajanNimi = t1.KustantajanNimi 
                        AND t2.JulkaisunTunnus <> t1.JulkaisunTunnus
                    LEFT JOIN julkaisut_ods.dbo.SA_JulkaisutTMP t3 ON t3.JulkaisunTunnus = t2.JulkaisunTunnus
                    WHERE " + with_where +
                    @"and t1.JulkaisutyyppiKoodi IN ('C1','D5','E1','E2')
                    and t2.JulkaisutyyppiKoodi IN ('C1','D5','E1','E2')
                    and lower(t1.JulkaisunNimi) NOT IN ('introduction','esipuhe','johdanto') 
                )
                UPDATE t1
                SET " +
                update_columns +
                "WHERE " + update_where;

                SqlConn.cmd.ExecuteNonQuery();
            }

            else if (ehto == 7)
            {
                // Tunnistussääntö 4:            
                SqlConn.cmd.CommandText = @"
                WITH t1 AS
                (
                    SELECT " +
                        with_columns +
                    @"FROM julkaisut_ods.dbo.SA_JulkaisutTMP t1 
                    INNER JOIN julkaisut_ods.dbo.ODS_JulkaisutTMP t2 ON t2.EmoJulkaisunNimi = t1.EmoJulkaisunNimi AND t2.JulkaisunNimi = t1.JulkaisunNimi AND t2.JulkaisunTunnus <> t1.JulkaisunTunnus 
                    LEFT JOIN julkaisut_ods.dbo.SA_JulkaisutTMP t3 ON t3.JulkaisunTunnus = t2.JulkaisunTunnus
                    WHERE " + with_where +
                    @"and t1.JulkaisutyyppiKoodi IN ('A3','A4','B2','B3','D1','D2','D3','E1')
                    and t2.JulkaisutyyppiKoodi IN ('A3','A4','B2','B3','D1','D2','D3','E1')
                    and lower(t1.JulkaisunNimi) NOT IN ('introduction','esipuhe','johdanto') 
                )
                UPDATE t1
                SET " +
                update_columns +
                "WHERE " + update_where;

                SqlConn.cmd.ExecuteNonQuery();
            }

            else if (ehto == 8)
            {
                // Tunnistussääntö 5 v.1:
                SqlConn.cmd.CommandText = @"
                WITH t1 AS
                (
                    SELECT " +
                        with_columns +
                    @"FROM julkaisut_ods.dbo.SA_JulkaisutTMP t1 
                    INNER JOIN julkaisut_ods.dbo.ODS_ISBN t4 ON t4.ISBN = t1.ISBN1 AND t4.JulkaisunTunnus != t1.JulkaisunTunnus
                    INNER JOIN julkaisut_ods.dbo.ODS_JulkaisutTMP t2 ON t2.JulkaisunTunnus = t4.JulkaisunTunnus AND t2.JulkaisunNimi = t1.JulkaisunNimi
                    LEFT JOIN julkaisut_ods.dbo.SA_JulkaisutTMP t3 ON t3.JulkaisunTunnus = t2.JulkaisunTunnus
                    WHERE " + with_where +
                @")
                UPDATE t1
                SET " +
                update_columns +
                "WHERE " + update_where;

                SqlConn.cmd.ExecuteNonQuery();
            }

            else if (ehto == 9)
            {
                // Tunnistussääntö 5 v.2:
                SqlConn.cmd.CommandText = @"
                WITH t1 AS
                (
                    SELECT " +
                        with_columns +
                    @"FROM julkaisut_ods.dbo.SA_JulkaisutTMP t1 
                    INNER JOIN julkaisut_ods.dbo.ODS_ISBN t4 ON t4.ISBN = t1.ISBN2 AND t4.JulkaisunTunnus != t1.JulkaisunTunnus
                    INNER JOIN julkaisut_ods.dbo.ODS_JulkaisutTMP t2 ON t2.JulkaisunTunnus = t4.JulkaisunTunnus AND t2.JulkaisunNimi = t1.JulkaisunNimi
                    LEFT JOIN julkaisut_ods.dbo.SA_JulkaisutTMP t3 ON t3.JulkaisunTunnus = t2.JulkaisunTunnus
                    WHERE " + with_where +
                @")
                UPDATE t1
                SET " +
                update_columns +
                "WHERE " + update_where;

                SqlConn.cmd.ExecuteNonQuery();
            }

            else if (ehto == 10)
            {
                // Tunnistussääntö 6:
                SqlConn.cmd.CommandText = @"
                WITH t1 AS
                (
                    SELECT " +
                        with_columns +
                    @"FROM julkaisut_ods.dbo.SA_JulkaisutTMP t1 
                    INNER JOIN julkaisut_ods.dbo.ODS_JulkaisutTMP t2 ON t2.JulkaisutyyppiKoodi = t1.JulkaisutyyppiKoodi 
                        AND t2.JulkaisunNimi = t1.JulkaisunNimi 
                        AND t2.lehdenNimi = t1.LehdenNimi 
                        AND t2.Julkaisuvuosi = t1.Julkaisuvuosi 
                        AND t2.JulkaisunTunnus <> t1.JulkaisunTunnus 
                    LEFT JOIN julkaisut_ods.dbo.SA_JulkaisutTMP t3 ON t3.JulkaisunTunnus = t2.JulkaisunTunnus
                    WHERE " + with_where +
                    @"and t1.JulkaisutyyppiKoodi = 'D1'
                    and t2.JulkaisutyyppiKoodi = 'D1'
                )
                UPDATE t1
                SET " +
                update_columns +
                "WHERE " + update_where;

                SqlConn.cmd.ExecuteNonQuery();
            }

            else if (ehto == 11)
            {
                // Tunnistussääntö 7:          
                SqlConn.cmd.CommandText = @"
                WITH t1 AS
                (
                    SELECT " +
                        with_columns +
                    @"FROM julkaisut_ods.dbo.SA_JulkaisutTMP t1 
                    INNER JOIN julkaisut_ods.dbo.ODS_JulkaisutTMP t2 ON t2.JulkaisutyyppiKoodi = t1.JulkaisutyyppiKoodi 
                        AND t2.JulkaisunNimi = t1.JulkaisunNimi 
                        AND t2.KustantajanNimi = t1.KustantajanNimi 
                        AND t2.Julkaisuvuosi = t1.Julkaisuvuosi 
                        AND t2.JulkaisunTunnus <> t1.JulkaisunTunnus 
                    LEFT JOIN julkaisut_ods.dbo.SA_JulkaisutTMP t3 ON t3.JulkaisunTunnus = t2.JulkaisunTunnus
                    WHERE " + with_where +
                    @"and t1.JulkaisutyyppiKoodi = 'D4' 
                    and t2.JulkaisutyyppiKoodi = 'D4' 
                )
                UPDATE t1
                SET " +
                update_columns +
                "WHERE " + update_where;

                SqlConn.cmd.ExecuteNonQuery();
            }

            else if (ehto == 12)
            {
                // Tunnistussääntö 8:   
                SqlConn.cmd.CommandText = @"
                WITH t1 AS
                (
                    SELECT " +
                        with_columns +
                    @"FROM julkaisut_ods.dbo.SA_JulkaisutTMP t1 
                    INNER JOIN julkaisut_ods.dbo.ODS_JulkaisutTMP t2 ON t2.JulkaisutyyppiKoodi = t1.JulkaisutyyppiKoodi 
                        AND t2.JulkaisunNimi = t1.JulkaisunNimi 
                        AND t2.AVsovellustyyppikoodi = t1.AVsovellustyyppikoodi 
                        AND t2.Julkaisuvuosi = t1.Julkaisuvuosi 
                        AND t2.JulkaisunTunnus <> t1.JulkaisunTunnus 
                    LEFT JOIN julkaisut_ods.dbo.SA_JulkaisutTMP t3 ON t3.JulkaisunTunnus = t2.JulkaisunTunnus
                    WHERE " + with_where +
                    @"and t1.JulkaisutyyppiKoodi in ('I1', 'I2') 
                    and t2.JulkaisutyyppiKoodi  in ('I1', 'I2') 
                )
                UPDATE t1
                SET " +
                update_columns +
                "WHERE " + update_where;

                SqlConn.cmd.ExecuteNonQuery();
            }

            else if (ehto == 13)
            {
                // Tunnistussääntö 9:       
                SqlConn.cmd.CommandText = @"
                WITH t1 AS
                (
                    SELECT " +
                        with_columns +
                    @"FROM julkaisut_ods.dbo.SA_JulkaisutTMP t1 
                    INNER JOIN julkaisut_ods.dbo.ODS_JulkaisutTMP t2 ON t2.JulkaisutyyppiKoodi = t1.JulkaisutyyppiKoodi 
                        AND t2.JulkaisunNimi = t1.JulkaisunNimi 
                        AND t2.Julkaisuvuosi = t1.Julkaisuvuosi 
                        AND t2.JulkaisunTunnus <> t1.JulkaisunTunnus 
                    LEFT JOIN julkaisut_ods.dbo.SA_JulkaisutTMP t3 ON t3.JulkaisunTunnus = t2.JulkaisunTunnus
                    WHERE " + with_where +
                    @"and t1.JulkaisutyyppiKoodi IN ('KA', 'KP')
                    and t2.JulkaisutyyppiKoodi IN ('KA', 'KP')
                )
                UPDATE t1
                SET " +
                update_columns +
                "WHERE " + update_where;

                SqlConn.cmd.ExecuteNonQuery();
            }

            SqlConn.Sulje();
        }


        public void Liputa_duplikaatit(int ehto)
        {
            SqlConn.Avaa();
            SqlConn.cmd.Parameters.AddWithValue("@ehto", ehto);

            // 1 Duplikaatti ei kuulu yhteisjulkaisuun
            SqlConn.cmd.CommandText = @"
                UPDATE t1
                SET t1.dupl_yhtjulk = 'dupl'
                FROM julkaisut_ods.dbo.SA_JulkaisutTMP t1
                INNER JOIN julkaisut_mds.koodi.JulkaisunTunnus t2 ON t2.JulkaisunTunnus = t1.dupl_JulkaisunTunnus
                WHERE t1.dupl_yhtjulk_ehto = @ehto
                and t1.OrganisaatioTunnus = t1.dupl_OrganisaatioTunnus
                and t1.JulkaisunOrgTunnus != t1.dupl_JulkaisunOrgTunnus
                and t2.Yhteisjulkaisu_ID = 0
                -- CSCVIRTAJTP-211
                and NOT ((t1.JulkaisunTilaKoodi = 3 AND (t1.JulkaisutyyppiKoodi = 'KA' OR t1.JulkaisutyyppiKoodi = 'KP')))";
            SqlConn.cmd.ExecuteNonQuery();

            // 2 Duplikaatti kuuluu yhteisjulkaisuun ja julkaisun organisaatio on mukana siinä
            // Samalla duplikaattijulkaisun tietojen päivitys
            SqlConn.cmd.CommandText = @"
                UPDATE t1
                SET 
                    t1.dupl_yhtjulk = 'dupl'
                    ,t1.dupl_JulkaisunTunnus = t3.JulkaisunTunnus
                    ,t1.dupl_OrganisaatioTunnus = t3.OrgTunnus
                    ,t1.dupl_JulkaisunOrgTunnus = t3.JulkaisunOrgTunnus
                FROM julkaisut_ods.dbo.SA_JulkaisutTMP t1
                INNER JOIN julkaisut_mds.koodi.JulkaisunTunnus t2 ON t2.JulkaisunTunnus = t1.dupl_JulkaisunTunnus
                INNER JOIN julkaisut_mds.koodi.julkaisuntunnus t3 ON t3.Yhteisjulkaisu_ID = t2.Yhteisjulkaisu_ID and t3.OrgTunnus = t1.OrganisaatioTunnus and t3.JulkaisunOrgTunnus != t1.JulkaisunOrgTunnus
                WHERE t1.dupl_yhtjulk_ehto = @ehto
                and t2.Yhteisjulkaisu_ID != 0
                -- CSCVIRTAJTP-211
                and NOT ((t1.JulkaisunTilaKoodi = 3 AND (t1.JulkaisutyyppiKoodi = 'KA' OR t1.JulkaisutyyppiKoodi = 'KP')))";
            SqlConn.cmd.ExecuteNonQuery();

            SqlConn.Sulje();
        }


        public void Liputa_yhteisjulkaisut(int ehto)
        {
            SqlConn.Avaa();
            SqlConn.cmd.Parameters.AddWithValue("@ehto", ehto);

            // 1 Duplikaatti ei kuulu yhteisjulkaisuun
            SqlConn.cmd.CommandText = @"
                UPDATE t1
                SET t1.dupl_yhtjulk = 'yhtjulk'
                FROM julkaisut_ods.dbo.SA_JulkaisutTMP t1
                INNER JOIN julkaisut_mds.koodi.JulkaisunTunnus t2 ON t2.JulkaisunTunnus = t1.dupl_JulkaisunTunnus
                WHERE t1.dupl_yhtjulk_ehto = @ehto
                and t1.OrganisaatioTunnus != t1.dupl_OrganisaatioTunnus
                and t2.Yhteisjulkaisu_ID = 0";
            SqlConn.cmd.ExecuteNonQuery();

            // 2 Duplikaatti kuuluu yhteisjulkaisuun mutta julkaisun organisaatio ei ole mukana siinä
            SqlConn.cmd.CommandText = @"
                UPDATE t1
                SET 
                     t1.dupl_yhtjulk = 'yhtjulk'
                    ,t1.Yhteisjulkaisu_ID = t2.Yhteisjulkaisu_ID
                FROM julkaisut_ods.dbo.SA_JulkaisutTMP t1
                INNER JOIN julkaisut_mds.koodi.JulkaisunTunnus t2 ON t2.JulkaisunTunnus = t1.dupl_JulkaisunTunnus
                LEFT JOIN julkaisut_mds.koodi.julkaisuntunnus t3 ON t3.Yhteisjulkaisu_ID = t2.Yhteisjulkaisu_ID and t3.OrgTunnus = t1.OrganisaatioTunnus
                WHERE t1.dupl_yhtjulk_ehto = @ehto
                and t2.Yhteisjulkaisu_ID != 0
                and t3.OrgTunnus is null";
            SqlConn.cmd.ExecuteNonQuery();

            SqlConn.Sulje();
        }


        public void Paivita_duplikaatit()
        {
            SqlConn.Avaa();
            SqlConn.cmd.Parameters.AddWithValue("@JulkaisunTilaKoodi", Globals.tilaKoodi_sisainen_duplikaatti);

            // Tilan päivitys
            SqlConn.cmd.CommandText = @"
                UPDATE t1
                SET t1.JulkaisunTilaKoodi = @JulkaisunTilaKoodi
                FROM julkaisut_ods.dbo.SA_Julkaisut t1
                INNER JOIN julkaisut_ods.dbo.SA_JulkaisutTMP t2 ON t2.JulkaisunTunnus = t1.JulkaisunTunnus
                 --#RT660639 
                LEFT JOIN julkaisut_ods.dbo.EiDuplikaattiTarkistusta t3 ON 
                    (t3.ekajulkorgtunnus=t2.JulkaisunOrgTunnus AND t3.tokajulkorgtunnus = t2.dupl_JulkaisunOrgTunnus) OR
                    (t3.tokajulkorgtunnus=t2.JulkaisunOrgTunnus AND t3.ekajulkorgtunnus = t2.dupl_JulkaisunOrgTunnus)
                WHERE t2.dupl_yhtjulk = 'dupl' AND t3.ID IS NULL";
            SqlConn.cmd.ExecuteNonQuery();
            SqlConn.Sulje();
        }


        public void Luo_yhteisjulkaisut()
        {
            SqlConn.Avaa();

            // Uusien Yhteisjulkaisu_ID:eiden generointi
            // Otetaan huomioon mahdollisuus että samassa satsissa on enemmän kuin yksi samaan yhteisjulkaisuun kuuluva osajulkaisu
            SqlConn.cmd.CommandText = @"
                WITH 
                FilteredRows AS (
	                SELECT 
		                 julkaisuntunnus
		                ,dupl_JulkaisunTunnus
		                ,Yhteisjulkaisu_ID
		                ,ROW_NUMBER() OVER (ORDER BY julkaisuntunnus DESC) as rn_julkaisuntunnus
                    FROM [julkaisut_ods].[dbo].[SA_JulkaisutTMP]
	                WHERE dupl_yhtjulk = 'yhtjulk' and Yhteisjulkaisu_ID is null
                )
                ,JoinedRows AS (
	                SELECT
		                R1.julkaisuntunnus
		                ,CASE
			                WHEN R1.rn_julkaisuntunnus > COALESCE(R2.rn_julkaisuntunnus, 0) THEN R1.dupl_JulkaisunTunnus
                            ELSE R2.dupl_JulkaisunTunnus
		                END AS dupl_JulkaisunTunnus
	                FROM FilteredRows R1
	                LEFT JOIN FilteredRows R2 ON R2.julkaisuntunnus = R1.dupl_JulkaisunTunnus
                )
                ,OrderedRows AS (
	                SELECT 
		                julkaisuntunnus
		                ,dupl_JulkaisunTunnus
		                ,DENSE_RANK() OVER (ORDER BY dupl_JulkaisunTunnus) AS rn_dupl_JulkaisunTunnus
	                FROM JoinedRows
                )
                ,MaxYhteisjulkaisu_ID AS (
                    SELECT MAX(Yhteisjulkaisu_ID) as max_id
                    FROM julkaisut_mds.koodi.JulkaisunTunnus
                )

                UPDATE R1
                SET Yhteisjulkaisu_ID = max_id + R2.rn_dupl_JulkaisunTunnus
                FROM FilteredRows R1
                INNER JOIN OrderedRows R2 ON R2.julkaisuntunnus = R1.julkaisuntunnus
                CROSS JOIN MaxYhteisjulkaisu_ID";

            SqlConn.cmd.ExecuteNonQuery();

            SqlConn.Sulje();
        }


        public void Paivita_yhteisjulkaisut()
        {
            SqlConn.Avaa();

            // Yhteisjulkaisu_ID:n päivitys
            SqlConn.cmd.CommandText = @"
                UPDATE t1
                SET t1.Yhteisjulkaisu_ID = t2.Yhteisjulkaisu_ID
                FROM julkaisut_mds.koodi.julkaisuntunnus t1
                INNER JOIN julkaisut_ods.dbo.SA_JulkaisutTMP t2 on t2.JulkaisunTunnus = t1.JulkaisunTunnus
                WHERE 1=1 --t1.Yhteisjulkaisu_ID = 0
                and t2.Yhteisjulkaisu_ID is not null";
            SqlConn.cmd.ExecuteNonQuery();

            // Generoidun yhteisjulkaisun toisen osapuolen Yhteisjulkaisu_ID:n päivitys
            SqlConn.cmd.CommandText = @"
                UPDATE t1
                SET t1.Yhteisjulkaisu_ID = t2.Yhteisjulkaisu_ID
                FROM julkaisut_mds.koodi.julkaisuntunnus t1
                INNER JOIN julkaisut_ods.dbo.SA_JulkaisutTMP t2 on t2.dupl_JulkaisunTunnus = t1.JulkaisunTunnus
                WHERE 1=1 --t1.Yhteisjulkaisu_ID = 0
                and t2.Yhteisjulkaisu_ID is not null";
            SqlConn.cmd.ExecuteNonQuery();

            SqlConn.Sulje();
        }


        public void Kirjoita_tarkistuslokiin()
        {
            SqlConn.Avaa();

            SqlConn.cmd.Parameters.AddWithValue("@TarkistusID_dupl", Globals.tarkistusID_sisainen_duplikaatti);
            SqlConn.cmd.Parameters.AddWithValue("@TarkistusID_yhtjulk", Globals.tarkistusID_yhteisjulkaisu);
            SqlConn.cmd.Parameters.AddWithValue("@tilaKoodiDupl", Globals.tilaKoodi_sisainen_duplikaatti);

            // Duplikaatit
            SqlConn.cmd.CommandText = @"
                INSERT INTO julkaisut_ods.dbo.Tarkistusloki (JulkaisunTunnus, JulkaisunOrgTunnus, OrganisaatioTunnus, LatausID, TarkistusID, Tila, Kuvaus, Lisatieto)
                SELECT
	                t1.JulkaisunTunnus
	                ,t1.JulkaisunOrgTunnus
	                ,t1.OrganisaatioTunnus
	                ,t1.Lataus_ID
	                ,@tarkistusId_dupl
	                ,@tilaKoodiDupl
	                ,dupl_JulkaisunOrgTunnus
                    ,dupl_yhtjulk_ehto
                FROM julkaisut_ods.dbo.SA_JulkaisutTMP t1
                --#RT660639 
                LEFT JOIN julkaisut_ods.dbo.EiDuplikaattiTarkistusta t2 ON 
                    (t2.ekajulkorgtunnus=t1.JulkaisunOrgTunnus AND t2.tokajulkorgtunnus = t1.dupl_JulkaisunOrgTunnus) OR
                    (t2.tokajulkorgtunnus=t1.JulkaisunOrgTunnus AND t2.ekajulkorgtunnus = t1.dupl_JulkaisunOrgTunnus)
                WHERE dupl_yhtjulk = 'dupl' AND t2.ID IS NULL";
            SqlConn.cmd.ExecuteNonQuery();

            // Yhteisjulkaisu
            SqlConn.cmd.CommandText = @"
                INSERT INTO julkaisut_ods.dbo.Tarkistusloki (JulkaisunTunnus, JulkaisunOrgTunnus, OrganisaatioTunnus, LatausID, TarkistusID, Tila, Kuvaus, Lisatieto)
                SELECT
	                JulkaisunTunnus
	                ,JulkaisunOrgTunnus
	                ,OrganisaatioTunnus
	                ,Lataus_ID
	                ,@tarkistusId_yhtjulk
	                ,JulkaisunTilaKoodi
	                ,Yhteisjulkaisu_ID
                    ,dupl_yhtjulk_ehto
                FROM julkaisut_ods.dbo.SA_JulkaisutTMP t1
                WHERE dupl_yhtjulk = 'yhtjulk'";
            SqlConn.cmd.ExecuteNonQuery();

            SqlConn.Sulje();
        }


    }

}