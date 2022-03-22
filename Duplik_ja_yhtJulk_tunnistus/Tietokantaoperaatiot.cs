using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace Duplik_ja_yhtJulk_tunnistus
{
    class Tietokantaoperaatiot
    {

        // julkaisut_ods.dbo.ODS_JulkaisutTMP
        // Poistetaan kaikki rivit ODS_JulkaisutTMP-taulusta
        public void tyhjenna_ODSjulkaisutTMP(string server)
        {

            string connectionString_ods_julkaisut = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";
            SqlConnection conn = new SqlConnection(connectionString_ods_julkaisut);

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            cmd.CommandText = "DELETE FROM dbo.ODS_JulkaisutTMP";
            cmd.CommandType = CommandType.Text;

            // asetetaan commandTimeout = 300 (oletus = 30), koska ohjelma kaatuu joskus commandTimeout -virheeseen
            cmd.CommandTimeout = 450;

            cmd.Connection = conn;

            cmd.ExecuteNonQuery();

            conn.Close();

        }


        // julkaisut_ods.dbo.ODS_JulkaisutTMP
        // haetaan rivit ODS_Julkaisut-taulusta ja lisataan ne ODS_JulkaisutTMP -tauluun
        public void insert_into_ODSjulkaisutTMP(string server, int vuosi)
        {

            string connectionString_ods_julkaisut = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";
            SqlConnection conn = new SqlConnection(connectionString_ods_julkaisut);

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            cmd.CommandText = "INSERT INTO dbo.ODS_JulkaisutTMP SELECT * FROM dbo.ODS_Julkaisut WHERE JulkaisuVuosi > @JulkaisuVuosi";
            cmd.CommandType = CommandType.Text;

            // asetetaan commandTimeout = 300 (oletus = 30), koska ohjelma kaatuu joskus commandTimeout -virheeseen
            cmd.CommandTimeout = 300;

            cmd.Connection = conn;

            // JulkaisuVuosi
            cmd.Parameters.AddWithValue("@JulkaisuVuosi", vuosi);

            cmd.ExecuteNonQuery();

            conn.Close();

        }


        // julkaisut_ods.dbo.ODS_JulkaisutTMP
        // haetaan seuraavat kentät:
        // JulkaisunNimi
        // KustantajanNimi
        // EmojulkaisunNimi
        // LehdenNimi
        public SqlDataReader ODS_JulkaisutTMP_hae_nimet(SqlConnection conn)
        {

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            cmd.CommandText = "SELECT JulkaisunTunnus, JulkaisunNimi, KustantajanNimi, EmojulkaisunNimi, DOI, Lehdennimi FROM dbo.ODS_JulkaisutTMP";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = conn;

            SqlDataReader reader = cmd.ExecuteReader();

            return reader;
        }


        // julkaisut_ods.dbo.ODS_JulkaisutTMP
        // muokataan parametrina annetun kuvauksen mukaista kenttaa
        public void ODS_JulkaisutTMP_update_nimi(string server, string julkTunnus, string nimi, string kuv)
        {

            string connectionString_ods_julkaisut = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_ods_julkaisut);

            conn.Open();

            using (conn)
            {

                SqlCommand cmd = new SqlCommand();

                // case 1: kuv = JulkaisunNimi
                if (kuv.Equals("JulkaisunNimi"))
                {
                    cmd = new SqlCommand("UPDATE dbo.ODS_JulkaisutTMP SET JulkaisunNimi = @JulkaisunNimi WHERE JulkaisunTunnus = @JulkaisunTunnus");
                    cmd.CommandType = CommandType.Text;

                    //////////////////////////////////////////////////////////////////////////////////////////////////////////
                    // asetetaan commandTimeout = 180 (oletus = 30), koska ohjelma kaatuu joskus commandTimeout -virheeseen //
                    //////////////////////////////////////////////////////////////////////////////////////////////////////////
                    cmd.CommandTimeout = 180;

                    cmd.Connection = conn;

                    // Nimi
                    if (String.IsNullOrEmpty(nimi))
                    {
                        cmd.Parameters.AddWithValue("@JulkaisunNimi", DBNull.Value);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@JulkaisunNimi", nimi);
                    }

                    // JulkaisunTunnus
                    if (String.IsNullOrEmpty(julkTunnus))
                    {
                        cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
                    }
                }

                // Case 2: kuv = KustantajanNimi
                else if (kuv.Equals("KustantajanNimi"))
                {
                    cmd = new SqlCommand("UPDATE dbo.ODS_JulkaisutTMP SET KustantajanNimi = @KustantajanNimi WHERE JulkaisunTunnus = @JulkaisunTunnus");
                    cmd.CommandType = CommandType.Text;

                    //////////////////////////////////////////////////////////////////////////////////////////////////////////
                    // asetetaan commandTimeout = 180 (oletus = 30), koska ohjelma kaatuu joskus commandTimeout -virheeseen //
                    //////////////////////////////////////////////////////////////////////////////////////////////////////////
                    cmd.CommandTimeout = 180;

                    cmd.Connection = conn;

                    // Nimi
                    if (String.IsNullOrEmpty(nimi))
                    {
                        cmd.Parameters.AddWithValue("@KustantajanNimi", DBNull.Value);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@KustantajanNimi", nimi);
                    }

                    // JulkaisunTunnus
                    if (String.IsNullOrEmpty(julkTunnus))
                    {
                        cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
                    }
                }

                // Case 3: kuv = EmojulkaisunNimi
                else if (kuv.Equals("EmojulkaisunNimi"))
                {
                    cmd = new SqlCommand("UPDATE dbo.ODS_JulkaisutTMP SET EmojulkaisunNimi = @EmojulkaisunNimi WHERE JulkaisunTunnus = @JulkaisunTunnus");
                    cmd.CommandType = CommandType.Text;

                    //////////////////////////////////////////////////////////////////////////////////////////////////////////
                    // asetetaan commandTimeout = 180 (oletus = 30), koska ohjelma kaatuu joskus commandTimeout -virheeseen //
                    //////////////////////////////////////////////////////////////////////////////////////////////////////////
                    cmd.CommandTimeout = 180;

                    cmd.Connection = conn;

                    // Nimi
                    if (String.IsNullOrEmpty(nimi))
                    {
                        cmd.Parameters.AddWithValue("@EmojulkaisunNimi", DBNull.Value);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@EmojulkaisunNimi", nimi);
                    }

                    // JulkaisunTunnus
                    if (String.IsNullOrEmpty(julkTunnus))
                    {
                        cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
                    }
                }

                // Case 4: kuv = DOI
                else if (kuv.Equals("DOI"))
                {
                    cmd = new SqlCommand("UPDATE dbo.ODS_JulkaisutTMP SET DOI = @DOI WHERE JulkaisunTunnus = @JulkaisunTunnus");
                    cmd.CommandType = CommandType.Text;

                    //////////////////////////////////////////////////////////////////////////////////////////////////////////
                    // asetetaan commandTimeout = 180 (oletus = 30), koska ohjelma kaatuu joskus commandTimeout -virheeseen //
                    //////////////////////////////////////////////////////////////////////////////////////////////////////////
                    cmd.CommandTimeout = 180;

                    cmd.Connection = conn;

                    // DOI
                    if (String.IsNullOrEmpty(nimi))
                    {
                        cmd.Parameters.AddWithValue("@DOI", DBNull.Value);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@DOI", nimi);
                    }

                    // JulkaisunTunnus
                    if (String.IsNullOrEmpty(julkTunnus))
                    {
                        cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
                    }
                }

                // Case 5: kuv = LehdenNimi
                else if (kuv.Equals("LehdenNimi"))
                {
                    cmd = new SqlCommand("UPDATE dbo.ODS_JulkaisutTMP SET LehdenNimi = @LehdenNimi WHERE JulkaisunTunnus = @JulkaisunTunnus");
                    cmd.CommandType = CommandType.Text;

                    //////////////////////////////////////////////////////////////////////////////////////////////////////////
                    // asetetaan commandTimeout = 180 (oletus = 30), koska ohjelma kaatuu joskus commandTimeout -virheeseen //
                    //////////////////////////////////////////////////////////////////////////////////////////////////////////
                    cmd.CommandTimeout = 180;

                    cmd.Connection = conn;

                    // LehdenNimi
                    if (String.IsNullOrEmpty(nimi))
                    {
                        cmd.Parameters.AddWithValue("@LehdenNimi", DBNull.Value);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@LehdenNimi", nimi);
                    }

                    // JulkaisunTunnus
                    if (String.IsNullOrEmpty(julkTunnus))
                    {
                        cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
                    }
                }


                cmd.ExecuteNonQuery();

            }

            conn.Close();

        }


        // [julkaisut_ods].[dbo].[SA_Julkaisut]
        //
        // Haetaan SA_Julkaisut-kannasta kaikki rivit
        // return SqlDataReader
        public SqlDataReader SA_julkaisut_hae_kaikki(SqlConnection conn, int vuosi)
        {

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            cmd.CommandText = "SELECT * FROM dbo.SA_Julkaisut WHERE JulkaisuVuosi > @JulkaisuVuosi";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = conn;

            cmd.Parameters.AddWithValue("@JulkaisuVuosi", vuosi);

            SqlDataReader reader = cmd.ExecuteReader();

            return reader;
        }


        // [julkaisut_ods].[dbo].[SA_ISSN]
        // [julkaisut_ods].[dbo].[SA_ISBN]
        // [julkaisut_ods].[dbo].[ODS_ISSN]
        // [julkaisut_ods].[dbo].[ODS_ISBN]
        //
        // Haetaan julkaisun ISSN- tai ISBN -tunnukset julkaisulle JulkaisunTunnus-arvon perusteella
        // Ensimmaine parametrina on tietokantayhteys ja toinen parametri kertoo mita tunnusta halutaan
        // (ISSN vai ISBN). Kolmas parametri kertoo halutaanko tunnukset SA- vai ODS -tauluista.
        public SqlDataReader hae_tunnus_julkaisulle(SqlConnection conn, string julkTunnus, string tunnus, string taulu)
        {

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            if (tunnus.Equals("ISSN") && taulu.Equals("SA"))
            {
                cmd.CommandText = "SELECT * FROM dbo.SA_ISSN WHERE JulkaisunTunnus = @JulkaisunTunnus";
            }
            else if (tunnus.Equals("ISBN") && taulu.Equals("SA"))
            {
                cmd.CommandText = "SELECT * FROM dbo.SA_ISBN WHERE JulkaisunTunnus = @JulkaisunTunnus";
            }
            else if (tunnus.Equals("ISSN") && taulu.Equals("ODS"))
            {
                cmd.CommandText = "SELECT * FROM dbo.ODS_ISSN WHERE JulkaisunTunnus = @JulkaisunTunnus";
            }
            else if (tunnus.Equals("ISBN") && taulu.Equals("ODS"))
            {
                cmd.CommandText = "SELECT * FROM dbo.ODS_ISBN WHERE JulkaisunTunnus = @JulkaisunTunnus";
            }

            cmd.CommandType = CommandType.Text;
            cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            cmd.Connection = conn;

            SqlDataReader reader = cmd.ExecuteReader();

            return reader;

        }


        // [julkaisut_ods].[dbo].[SA_ISSN]
        // [julkaisut_ods].[dbo].[SA_ISBN]
        // [julkaisut_ods].[dbo].[ODS_ISSN]
        // [julkaisut_ods].[dbo].[ODS_ISBN]
        //
        // laske parametrina annetun julkaisun ISSN- tai ISBN -tunnusten maara. Toisena parametrina annetaan tieto
        // halutaanko ISSN- vai ISBN-tunnusten maarat ja kolmas parametri kertoo, mista taulusta haetaan tieto (SA/ODS)
        public int count_tunnusten_maara(string server, string julkTunnus, string tunnus, string taulu)
        {

            string connectionString_ods_julkaisut = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_ods_julkaisut);

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            if (tunnus.Equals("ISSN") && taulu.Equals("SA"))
            {
                cmd.CommandText = "SELECT COUNT(*) FROM dbo.SA_ISSN WHERE JulkaisunTunnus = @JulkaisunTunnus";
            }
            else if (tunnus.Equals("ISBN") && taulu.Equals("SA"))
            {
                cmd.CommandText = "SELECT COUNT(*) FROM dbo.SA_ISBN WHERE JulkaisunTunnus = @JulkaisunTunnus";
            }
            else if (tunnus.Equals("ISSN") && taulu.Equals("ODS"))
            {
                cmd.CommandText = "SELECT COUNT(*) FROM dbo.ODS_ISSN WHERE JulkaisunTunnus = @JulkaisunTunnus";
            }
            else if (tunnus.Equals("ISBN") && taulu.Equals("ODS"))
            {
                cmd.CommandText = "SELECT COUNT(*) FROM dbo.ODS_ISBN WHERE JulkaisunTunnus = @JulkaisunTunnus";
            }

            cmd.CommandType = CommandType.Text;
            cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            cmd.Connection = conn;

            int maara = (int)cmd.ExecuteScalar();

            conn.Close();

            return maara;
        }


        public int loytyy_yhteisjulkaisu(string server, string julkTunnus, string doi, string issn1, string issn2, string isbn1, string isbn2, string volyymi, string numero, string sivut, string julkNimi, string julkTyyppi, string kustantaja, string emoJulkNimi, string lehdenNimi, int vuosi, int ODStilaKoodi)
        {

            // vakio, joka palautetaan sen perusteella, mika match on kyseessa
            // vakioita 4 ja 5 ei enaa tarkastella, koska Tunnistussääntöihin 2 ja 3 lisattiin julkaisun nimi, 
            // jota ei aikaisemmin ollut ko. ehdoissa
            int loytyy = 0; // 0 = ei matchia
            int loytyy_DOI = 1;
            int loytyy_ISSN1_volyymi_numero_sivut_julkaisunNimi = 2;
            int loytyy_ISSN2_volyymi_numero_sivut_julkaisunNimi = 3;
            //int loytyy_ISSN1_volyymi_numero_julkaisunNimi = 4;
            //int loytyy_ISSN2_volyymi_numero_julkaisunNimi = 5;
            int loytyy_julkaisutyyppi_julkaisunNimi_kustantaja = 6; // koskee julkaisutyyppeja C1, D5, E2, pl. introduction, esipuhe, johdanto
            int loytyy_emoJulkaisunNimi_julkaisunNimi = 7; // koskee julkaisutyyppeja A3, A4, B2, B3, D1, D2, D3, E1, pl. introduction, esipuhe, johdanto
            int loytyy_ISBN1_julkaisunNimi = 8;
            int loytyy_ISBN2_julkaisunNimi = 9;
            int loytyy_julkaisutyyppi_julkaisunNimi_lehdenNimi_julkaisuvuosi = 10; // koskee julkaisutyyppeja D1
            int loytyy_julkaisutyyppi_julkaisunNimi_kustantajanNimi_julkaisuvuosi = 11; // koskee julkaisutyyppeja D4


            string connectionString_ods_julkaisut = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_ods_julkaisut);

            conn.Open();

            ///////////////////////////////////////////
            // Tunnistussääntö 1: kysely ehdolla DOI //
            /////////////////////////////////////////// 
            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.Text;

            //////////////////////////////////////////////////////////////////////////////////////////////////////////
            // asetetaan commandTimeout = 240 (oletus = 30), koska ohjelma kaatuu joskus commandTimeout -virheeseen //
            //////////////////////////////////////////////////////////////////////////////////////////////////////////
            //Eri tarkastusehdot julkaisutyypistä riippuen 11.2.2022
            cmd.CommandTimeout = 240;

            cmd.CommandText = "SELECT COUNT(*) FROM dbo.ODS_JulkaisutTMP WHERE DOI = @DOI AND JulkaisunTunnus <> @JulkaisunTunnus AND JulkaisunTilaKoodi > @JulkaisunTilaKoodi";

            if (String.IsNullOrEmpty(doi))
            {
                cmd.Parameters.AddWithValue("@DOI", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@DOI", doi);
            }

            if (String.IsNullOrEmpty(julkTunnus))
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            }


            cmd.Parameters.AddWithValue("@JulkaisunTilaKoodi", ODStilaKoodi);

            cmd.Connection = conn;

            loytyy = (int)cmd.ExecuteScalar();

            if (loytyy > 0)
            {

                conn.Close();

                return loytyy_DOI;
            }





            //////////////////////////////////////////////////////////////////////////////
            // Tunnistussääntö 2 variaatio 1 : kysely ehdolla ISSN1 + volyymi + numero + sivut + julkaisun nimi //
            //////////////////////////////////////////////////////////////////////////////
            cmd = new SqlCommand();
            cmd.CommandType = CommandType.Text;

            cmd.CommandText = "SELECT COUNT(*) FROM [dbo].[ODS_ISSN] WHERE ISSN = @ISSN AND JulkaisunTunnus IN" +
                "(SELECT JulkaisunTunnus FROM dbo.ODS_JulkaisutTMP WHERE SivunumeroTeksti = @SivunumeroTeksti AND VolyymiTeksti = @VolyymiTeksti AND LehdenNumeroTeksti = @LehdenNumeroTeksti AND JulkaisunNimi = @JulkaisunNimi AND JulkaisunTunnus <> @JulkaisunTunnus AND JulkaisunTilaKoodi > @JulkaisunTilaKoodi)";

            if (String.IsNullOrEmpty(issn1))
            {
                cmd.Parameters.AddWithValue("@ISSN", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@ISSN", issn1);
            }

            if (String.IsNullOrEmpty(volyymi))
            {
                cmd.Parameters.AddWithValue("@VolyymiTeksti", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@VolyymiTeksti", volyymi);
            }

            if (String.IsNullOrEmpty(numero))
            {
                cmd.Parameters.AddWithValue("@LehdenNumeroTeksti", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@LehdenNumeroTeksti", numero);
            }

            if (String.IsNullOrEmpty(sivut))
            {
                cmd.Parameters.AddWithValue("@SivunumeroTeksti", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@SivunumeroTeksti", sivut);
            }

            if (String.IsNullOrEmpty(julkNimi))
            {
                cmd.Parameters.AddWithValue("@JulkaisunNimi", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunNimi", julkNimi);
            }

            if (String.IsNullOrEmpty(julkTunnus))
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            }

            cmd.Parameters.AddWithValue("@JulkaisunTilaKoodi", ODStilaKoodi);

            cmd.Connection = conn;

            loytyy = (int)cmd.ExecuteScalar();

            if (loytyy > 0)
            {

                conn.Close();

                return loytyy_ISSN1_volyymi_numero_sivut_julkaisunNimi;
            }


            //////////////////////////////////////////////////////////////////////////////
            // Tunnistussääntö  2 variaatio 2: kysely ehdolla ISSN2 + volyymi + numero + sivut + julkaisun nimi //
            //////////////////////////////////////////////////////////////////////////////
            cmd = new SqlCommand();
            cmd.CommandType = CommandType.Text;

            cmd.CommandText = "SELECT COUNT(*) FROM [dbo].[ODS_ISSN] WHERE ISSN = @ISSN AND JulkaisunTunnus IN" +
                "(SELECT JulkaisunTunnus FROM dbo.ODS_JulkaisutTMP WHERE SivunumeroTeksti = @SivunumeroTeksti AND VolyymiTeksti = @VolyymiTeksti AND LehdenNumeroTeksti = @LehdenNumeroTeksti AND JulkaisunNimi = @JulkaisunNimi AND JulkaisunTunnus <> @JulkaisunTunnus AND JulkaisunTilaKoodi > @JulkaisunTilaKoodi)";

            if (String.IsNullOrEmpty(issn2))
            {
                cmd.Parameters.AddWithValue("@ISSN", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@ISSN", issn2);
            }

            if (String.IsNullOrEmpty(volyymi))
            {
                cmd.Parameters.AddWithValue("@VolyymiTeksti", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@VolyymiTeksti", volyymi);
            }

            if (String.IsNullOrEmpty(numero))
            {
                cmd.Parameters.AddWithValue("@LehdenNumeroTeksti", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@LehdenNumeroTeksti", numero);
            }

            if (String.IsNullOrEmpty(sivut))
            {
                cmd.Parameters.AddWithValue("@SivunumeroTeksti", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@SivunumeroTeksti", sivut);
            }

            if (String.IsNullOrEmpty(julkNimi))
            {
                cmd.Parameters.AddWithValue("@JulkaisunNimi", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunNimi", julkNimi);
            }

            if (String.IsNullOrEmpty(julkTunnus))
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            }

            cmd.Parameters.AddWithValue("@JulkaisunTilaKoodi", ODStilaKoodi);

            cmd.Connection = conn;

            loytyy = (int)cmd.ExecuteScalar();

            if (loytyy > 0)
            {

                conn.Close();

                return loytyy_ISSN2_volyymi_numero_sivut_julkaisunNimi;
            }


            //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // Tunnistussääntö 3: kysely ehdolla julkaisutyyppi + julkaisun nimi + kustantaja (koskee julkaisutyyppeja C1, D5, E2, pl. Introduction, Esipuhe, Johdanto) //
            //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            if (julkTyyppi.Equals("C1") || julkTyyppi.Equals("D5") || julkTyyppi.Equals("E2"))
            {

                if (!(julkNimi.Equals("introduction")) && !(julkNimi.Equals("esipuhe")) && !(julkNimi.Equals("johdanto")))
                {

                    cmd = new SqlCommand();
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = "SELECT COUNT(*) FROM dbo.ODS_JulkaisutTMP WHERE JulkaisutyyppiKoodi = @JulkaisutyyppiKoodi AND JulkaisunNimi = @JulkaisunNimi AND KustantajanNimi = @KustantajanNimi AND JulkaisunTunnus <> @JulkaisunTunnus AND JulkaisunTilaKoodi > @JulkaisunTilaKoodi";

                    if (String.IsNullOrEmpty(julkTyyppi))
                    {
                        cmd.Parameters.AddWithValue("@JulkaisutyyppiKoodi", DBNull.Value);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@JulkaisutyyppiKoodi", julkTyyppi);
                    }

                    if (String.IsNullOrEmpty(julkNimi))
                    {
                        cmd.Parameters.AddWithValue("@JulkaisunNimi", DBNull.Value);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@JulkaisunNimi", julkNimi);
                    }

                    if (String.IsNullOrEmpty(kustantaja))
                    {
                        cmd.Parameters.AddWithValue("@KustantajanNimi", DBNull.Value);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@KustantajanNimi", kustantaja);
                    }

                    if (String.IsNullOrEmpty(julkTunnus))
                    {
                        cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
                    }

                    cmd.Parameters.AddWithValue("@JulkaisunTilaKoodi", ODStilaKoodi);

                    cmd.Connection = conn;

                    loytyy = (int)cmd.ExecuteScalar();

                    if (loytyy > 0)
                    {

                        conn.Close();

                        return loytyy_julkaisutyyppi_julkaisunNimi_kustantaja;
                    }

                }

            }


            /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // Tunnistussääntö 4: kysely ehdolla emojulkaisun nimi + julkaisun nimi (koskee julkaisutyyppeja A3, A4, B2, B3, D1, D2, D3,  E1, pl. Introduction, Esipuhe, Johdanto) //
            /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            if (julkTyyppi.Equals("A3") || julkTyyppi.Equals("A4") || julkTyyppi.Equals("B2") || julkTyyppi.Equals("B3") || julkTyyppi.Equals("D1") || julkTyyppi.Equals("D2") || julkTyyppi.Equals("D3") || julkTyyppi.Equals("E1"))
            {

                if (!(julkNimi.Equals("introduction")) && !(julkNimi.Equals("esipuhe")) && !(julkNimi.Equals("johdanto")))
                {

                    cmd = new SqlCommand();
                    cmd.CommandType = CommandType.Text;

                    cmd.CommandText = "SELECT COUNT(*) FROM dbo.ODS_JulkaisutTMP WHERE EmoJulkaisunNimi = @EmoJulkaisunNimi AND JulkaisunNimi = @JulkaisunNimi AND JulkaisunTunnus <> @JulkaisunTunnus AND JulkaisunTilaKoodi > @JulkaisunTilaKoodi";

                    if (String.IsNullOrEmpty(emoJulkNimi))
                    {
                        cmd.Parameters.AddWithValue("@EmoJulkaisunNimi", DBNull.Value);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@EmoJulkaisunNimi", emoJulkNimi);
                    }

                    if (String.IsNullOrEmpty(julkNimi))
                    {
                        cmd.Parameters.AddWithValue("@JulkaisunNimi", DBNull.Value);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@JulkaisunNimi", julkNimi);
                    }

                    if (String.IsNullOrEmpty(julkTunnus))
                    {
                        cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
                    }

                    cmd.Parameters.AddWithValue("@JulkaisunTilaKoodi", ODStilaKoodi);

                    cmd.Connection = conn;

                    loytyy = (int)cmd.ExecuteScalar();

                    if (loytyy > 0)
                    {

                        conn.Close();

                        return loytyy_emoJulkaisunNimi_julkaisunNimi;
                    }

                }

            }


            ///////////////////////////////////////////////////
            // Tunnistussääntö 5 v.1: kysely ehdolla ISBN1 + julkaisun nimi //
            ///////////////////////////////////////////////////
            cmd = new SqlCommand();
            cmd.CommandType = CommandType.Text;

            cmd.CommandText = "SELECT COUNT(*) FROM [dbo].[ODS_ISBN] WHERE ISBN = @ISBN AND JulkaisunTunnus IN" +
                "(SELECT JulkaisunTunnus FROM dbo.ODS_JulkaisutTMP WHERE JulkaisunNimi = @JulkaisunNimi AND JulkaisunTunnus <> @JulkaisunTunnus AND JulkaisunTilaKoodi > @JulkaisunTilaKoodi)";

            if (String.IsNullOrEmpty(isbn1))
            {
                cmd.Parameters.AddWithValue("@ISBN", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@ISBN", isbn1);
            }

            if (String.IsNullOrEmpty(julkNimi))
            {
                cmd.Parameters.AddWithValue("@JulkaisunNimi", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunNimi", julkNimi);
            }

            if (String.IsNullOrEmpty(julkTunnus))
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            }

            cmd.Parameters.AddWithValue("@JulkaisunTilaKoodi", ODStilaKoodi);

            cmd.Connection = conn;

            loytyy = (int)cmd.ExecuteScalar();

            if (loytyy > 0)
            {

                conn.Close();

                return loytyy_ISBN1_julkaisunNimi;
            }


            ///////////////////////////////////////////////////////
            // Tunnistussääntö 5 v.2: kysely ehdolla ISBN2 + julkaisun nimi //
            ///////////////////////////////////////////////////////
            cmd = new SqlCommand();
            cmd.CommandType = CommandType.Text;

            cmd.CommandText = "SELECT COUNT(*) FROM [dbo].[ODS_ISBN] WHERE ISBN = @ISBN AND JulkaisunTunnus IN" +
                "(SELECT JulkaisunTunnus FROM dbo.ODS_JulkaisutTMP WHERE JulkaisunNimi = @JulkaisunNimi AND JulkaisunTunnus <> @JulkaisunTunnus AND JulkaisunTilaKoodi > @JulkaisunTilaKoodi)";

            if (String.IsNullOrEmpty(isbn2))
            {
                cmd.Parameters.AddWithValue("@ISBN", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@ISBN", isbn2);
            }

            if (String.IsNullOrEmpty(julkNimi))
            {
                cmd.Parameters.AddWithValue("@JulkaisunNimi", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunNimi", julkNimi);
            }

            if (String.IsNullOrEmpty(julkTunnus))
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            }

            cmd.Parameters.AddWithValue("@JulkaisunTilaKoodi", ODStilaKoodi);

            cmd.Connection = conn;

            loytyy = (int)cmd.ExecuteScalar();

            if (loytyy > 0)
            {

                conn.Close();

                return loytyy_ISBN2_julkaisunNimi;
            }



            ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // Tunnistussääntö 6: kysely ehdolla julkaisutyyppi + julkaisun nimi + lehden nimi + julkaisuvuosi (koskee julkaisutyyppejaD1 //
            ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            if (julkTyyppi.Equals("D1"))
            {
                cmd = new SqlCommand();
                cmd.CommandType = CommandType.Text;
                //julkaisutyyppi + julkaisun nimi + lehden nimi + julkaisuvuosi
                cmd.CommandText = "SELECT COUNT(*) FROM dbo.ODS_JulkaisutTMP WHERE JulkaisutyyppiKoodi = @JulkaisutyyppiKoodi AND  " +
                "JulkaisunNimi = @JulkaisunNimi AND  lehdenNimi= @LehdenNimi AND Julkaisuvuosi = @Julkaisuvuosi AND " +
                "JulkaisunTunnus <> @JulkaisunTunnus AND JulkaisunTilaKoodi > @JulkaisunTilaKoodi";

                if (String.IsNullOrEmpty(julkTyyppi))
                {
                    cmd.Parameters.AddWithValue("@JulkaisutyyppiKoodi", DBNull.Value);
                }
                else
                {
                    cmd.Parameters.AddWithValue("@JulkaisutyyppiKoodi", julkTyyppi);
                }

                if (String.IsNullOrEmpty(julkNimi))
                {
                    cmd.Parameters.AddWithValue("@JulkaisunNimi", DBNull.Value);
                }
                else
                {
                    cmd.Parameters.AddWithValue("@JulkaisunNimi", julkNimi);
                }

                if (String.IsNullOrEmpty(lehdenNimi))
                {
                    cmd.Parameters.AddWithValue("@LehdenNimi", DBNull.Value);
                }
                else
                {
                    cmd.Parameters.AddWithValue("@LehdenNimi", lehdenNimi);
                }


                cmd.Parameters.AddWithValue("@Julkaisuvuosi", vuosi);

                if (String.IsNullOrEmpty(julkTunnus))
                {
                    cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
                }
                else
                {
                    cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
                }

                cmd.Parameters.AddWithValue("@JulkaisunTilaKoodi", ODStilaKoodi);

                cmd.Connection = conn;

                loytyy = (int)cmd.ExecuteScalar();

                if (loytyy > 0)
                {

                    conn.Close();

                    return loytyy_julkaisutyyppi_julkaisunNimi_lehdenNimi_julkaisuvuosi;
                }

            }

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // Tunnistussääntö 7: kysely ehdolla julkaisutyyppi + julkaisun nimi + kustantajan nimi + julkaisuvuosi (koskee julkaisutyyppeja D4 //
            ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            if (julkTyyppi.Equals("D4"))
            {
                cmd = new SqlCommand();
                cmd.CommandType = CommandType.Text;
                //julkaisutyyppi + julkaisun nimi + lehden nimi + julkaisuvuosi
                cmd.CommandText = "SELECT COUNT(*) FROM dbo.ODS_JulkaisutTMP WHERE JulkaisutyyppiKoodi = @JulkaisutyyppiKoodi AND  " +
                "JulkaisunNimi = @JulkaisunNimi AND  KustantajanNimi= @KustantajanNimi AND Julkaisuvuosi = @Julkaisuvuosi AND " +
                "JulkaisunTunnus <> @JulkaisunTunnus AND JulkaisunTilaKoodi > @JulkaisunTilaKoodi";

                if (String.IsNullOrEmpty(julkTyyppi))
                {
                    cmd.Parameters.AddWithValue("@JulkaisutyyppiKoodi", DBNull.Value);
                }
                else
                {
                    cmd.Parameters.AddWithValue("@JulkaisutyyppiKoodi", julkTyyppi);
                }

                if (String.IsNullOrEmpty(julkNimi))
                {
                    cmd.Parameters.AddWithValue("@JulkaisunNimi", DBNull.Value);
                }
                else
                {
                    cmd.Parameters.AddWithValue("@JulkaisunNimi", julkNimi);
                }

                if (String.IsNullOrEmpty(kustantaja))
                {
                    cmd.Parameters.AddWithValue("@KustantajanNimi", DBNull.Value);
                }
                else
                {
                    cmd.Parameters.AddWithValue("@KustantajanNimi", kustantaja);
                }


                cmd.Parameters.AddWithValue("@Julkaisuvuosi", vuosi);

                if (String.IsNullOrEmpty(julkTunnus))
                {
                    cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
                }
                else
                {
                    cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
                }

                cmd.Parameters.AddWithValue("@JulkaisunTilaKoodi", ODStilaKoodi);

                cmd.Connection = conn;

                loytyy = (int)cmd.ExecuteScalar();

                if (loytyy > 0)
                {

                    conn.Close();

                    return loytyy_julkaisutyyppi_julkaisunNimi_kustantajanNimi_julkaisuvuosi;
                }

            }

            conn.Close();

            return loytyy;  // tanne mennaan, jos ei loydy matcheja (= yhteisjulkaisuja) eli loytyy = 0

        }

        /* Tasta alkavat haut sen perusteella, minka avulla matcheja saatiin */

        // Tunnistussääntö 1:
        public SqlDataReader haku_ODS_alueelta_DOI(SqlConnection conn, string julkTunnus, string doi, int ODStilaKoodi)
        {

            conn.Open();

            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.Text;

            cmd.CommandText = "SELECT JulkaisunTunnus, JulkaisunOrgTunnus, OrganisaatioTunnus, JulkaisutyyppiKoodi, JulkaisunNimi FROM dbo.ODS_JulkaisutTMP WHERE DOI = @DOI AND JulkaisunTunnus <> @JulkaisunTunnus AND JulkaisunTilaKoodi > @JulkaisunTilaKoodi";

            if (String.IsNullOrEmpty(doi))
            {
                cmd.Parameters.AddWithValue("@DOI", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@DOI", doi);
            }

            if (String.IsNullOrEmpty(julkTunnus))
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            }

            cmd.Parameters.AddWithValue("@JulkaisunTilaKoodi", ODStilaKoodi);

            cmd.Connection = conn;

            SqlDataReader reader = cmd.ExecuteReader();

            return reader;
        }




        // Tunnistussääntö 2 v.1 :
        // haetaan vain julkaisunTunnus ISSN1-tunnuksen perusteella
        public string haku_ODS_alueelta_ISSN1_volyymi_numero_sivut_julkaisunNimi(string server, string julkTunnus, string issn1, string volyymi, string numero, string sivut, string julkNimi, int ODStilaKoodi)
        {

            string connectionString_ods_julkaisut = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_ods_julkaisut);

            conn.Open();

            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.Text;

            cmd.CommandText = "SELECT TOP 1 JulkaisunTunnus FROM [dbo].[ODS_ISSN] WHERE ISSN = @ISSN AND JulkaisunTunnus IN" +
                "(SELECT JulkaisunTunnus FROM dbo.ODS_JulkaisutTMP WHERE SivunumeroTeksti = @SivunumeroTeksti AND VolyymiTeksti = @VolyymiTeksti AND LehdenNumeroTeksti = @LehdenNumeroTeksti AND JulkaisunNimi = @JulkaisunNimi AND JulkaisunTunnus <> @JulkaisunTunnus AND JulkaisunTilaKoodi > @JulkaisunTilaKoodi)";

            if (String.IsNullOrEmpty(issn1))
            {
                cmd.Parameters.AddWithValue("@ISSN", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@ISSN", issn1);
            }

            if (String.IsNullOrEmpty(volyymi))
            {
                cmd.Parameters.AddWithValue("@VolyymiTeksti", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@VolyymiTeksti", volyymi);
            }

            if (String.IsNullOrEmpty(numero))
            {
                cmd.Parameters.AddWithValue("@LehdenNumeroTeksti", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@LehdenNumeroTeksti", numero);
            }

            if (String.IsNullOrEmpty(sivut))
            {
                cmd.Parameters.AddWithValue("@SivunumeroTeksti", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@SivunumeroTeksti", sivut);
            }

            if (String.IsNullOrEmpty(julkNimi))
            {
                cmd.Parameters.AddWithValue("@JulkaisunNimi", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunNimi", julkNimi);
            }

            if (String.IsNullOrEmpty(julkTunnus))
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            }

            cmd.Parameters.AddWithValue("@JulkaisunTilaKoodi", ODStilaKoodi);

            cmd.Connection = conn;

            string tunnus = cmd.ExecuteScalar().ToString();

            conn.Close();

            return tunnus;

        }


        // Tunnistussääntö 2 v.2:
        // haetaan vain julkaisunTunnus ISSN2-tunnuksen perusteella
        public string haku_ODS_alueelta_ISSN2_volyymi_numero_sivut_julkaisunNimi(string server, string julkTunnus, string issn2, string volyymi, string numero, string sivut, string julkNimi, int ODStilaKoodi)
        {

            string connectionString_ods_julkaisut = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_ods_julkaisut);

            conn.Open();

            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.Text;

            cmd.CommandText = "SELECT TOP 1 JulkaisunTunnus FROM [dbo].[ODS_ISSN] WHERE ISSN = @ISSN AND JulkaisunTunnus IN" +
                "(SELECT JulkaisunTunnus FROM dbo.ODS_JulkaisutTMP WHERE SivunumeroTeksti = @SivunumeroTeksti AND VolyymiTeksti = @VolyymiTeksti AND LehdenNumeroTeksti = @LehdenNumeroTeksti AND JulkaisunNimi = @JulkaisunNimi AND JulkaisunTunnus <> @JulkaisunTunnus AND JulkaisunTilaKoodi > @JulkaisunTilaKoodi)";

            if (String.IsNullOrEmpty(issn2))
            {
                cmd.Parameters.AddWithValue("@ISSN", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@ISSN", issn2);
            }

            if (String.IsNullOrEmpty(volyymi))
            {
                cmd.Parameters.AddWithValue("@VolyymiTeksti", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@VolyymiTeksti", volyymi);
            }

            if (String.IsNullOrEmpty(numero))
            {
                cmd.Parameters.AddWithValue("@LehdenNumeroTeksti", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@LehdenNumeroTeksti", numero);
            }

            if (String.IsNullOrEmpty(sivut))
            {
                cmd.Parameters.AddWithValue("@SivunumeroTeksti", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@SivunumeroTeksti", sivut);
            }

            if (String.IsNullOrEmpty(julkNimi))
            {
                cmd.Parameters.AddWithValue("@JulkaisunNimi", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunNimi", julkNimi);
            }

            if (String.IsNullOrEmpty(julkTunnus))
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            }

            cmd.Parameters.AddWithValue("@JulkaisunTilaKoodi", ODStilaKoodi);

            cmd.Connection = conn;

            string tunnus = cmd.ExecuteScalar().ToString();

            conn.Close();

            return tunnus;

        }


        // Haetaan JulkaisunOrgTunnus parametrina annetulle julkaisunTunnukselle
        public string hae_julkaisunOrgTunnus_julkaisunTunnuksella(string server, string julkTunnus)
        {

            string connectionString_ods_julkaisut = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_ods_julkaisut);

            conn.Open();

            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.Text;

            cmd.CommandText = "SELECT JulkaisunOrgTunnus FROM dbo.ODS_JulkaisutTMP WHERE JulkaisunTunnus = @JulkaisunTunnus";

            if (String.IsNullOrEmpty(julkTunnus))
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            }

            cmd.Connection = conn;

            string tunnus = cmd.ExecuteScalar().ToString();

            conn.Close();

            return tunnus;

        }


        // Haetaan organisaatiotunnus parametrina annetulle julkaisunTunnukselle
        public string hae_organisaatioTunnus_julkaisunTunnuksella(string server, string julkTunnus)
        {

            string connectionString_ods_julkaisut = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_ods_julkaisut);

            conn.Open();

            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.Text;

            cmd.CommandText = "SELECT OrganisaatioTunnus FROM dbo.ODS_JulkaisutTMP WHERE JulkaisunTunnus = @JulkaisunTunnus";

            if (String.IsNullOrEmpty(julkTunnus))
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            }

            cmd.Connection = conn;

            string tunnus = cmd.ExecuteScalar().ToString();

            conn.Close();

            return tunnus;

        }


        // Haetaan julkaisutyyppi parametrina annetulle julkaisunTunnukselle
        public string hae_julkaisutyyppi_julkaisunTunnuksella(string server, string julkTunnus)
        {

            string connectionString_ods_julkaisut = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_ods_julkaisut);

            conn.Open();

            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.Text;

            cmd.CommandText = "SELECT JulkaisutyyppiKoodi FROM dbo.ODS_JulkaisutTMP WHERE JulkaisunTunnus = @JulkaisunTunnus";

            if (String.IsNullOrEmpty(julkTunnus))
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            }

            cmd.Connection = conn;

            string julkaisutyyppi = cmd.ExecuteScalar().ToString();

            conn.Close();

            return julkaisutyyppi;

        }


        // Haetaan DOI parametrina annetulle julkaisunTunnukselle
        public string hae_DOI_julkaisunTunnuksella(string server, string julkTunnus)
        {

            string connectionString_ods_julkaisut = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_ods_julkaisut);

            conn.Open();

            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.Text;

            cmd.CommandText = "SELECT DOI FROM dbo.ODS_JulkaisutTMP WHERE JulkaisunTunnus = @JulkaisunTunnus";

            if (String.IsNullOrEmpty(julkTunnus))
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            }

            cmd.Connection = conn;

            string DOI = cmd.ExecuteScalar().ToString();

            conn.Close();

            return DOI;

        }


        // Tunnistussääntö 3:
        public SqlDataReader haku_ODS_alueelta_julkTyyppi_julkNimi_kustantaja(SqlConnection conn, string julkTunnus, string julkTyyppi, string julkNimi, string kustantaja, int ODStilaKoodi)
        {

            if (julkTyyppi.Equals("C1") || julkTyyppi.Equals("D5") || julkTyyppi.Equals("E2"))
            {

                if (!(julkNimi.Equals("introduction")) && !(julkNimi.Equals("esipuhe")) && !(julkNimi.Equals("johdanto")))
                {

                    conn.Open();

                    SqlCommand cmd = new SqlCommand();
                    cmd.CommandType = CommandType.Text;

                    cmd.CommandText = "SELECT JulkaisunTunnus, JulkaisunOrgTunnus, OrganisaatioTunnus, JulkaisutyyppiKoodi FROM dbo.ODS_JulkaisutTMP WHERE JulkaisutyyppiKoodi = @JulkaisutyyppiKoodi AND JulkaisunNimi = @JulkaisunNimi AND KustantajanNimi = @KustantajanNimi AND JulkaisunTunnus <> @JulkaisunTunnus AND JulkaisunTilaKoodi > @JulkaisunTilaKoodi";

                    if (String.IsNullOrEmpty(julkTyyppi))
                    {
                        cmd.Parameters.AddWithValue("@JulkaisutyyppiKoodi", DBNull.Value);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@JulkaisutyyppiKoodi", julkTyyppi);
                    }

                    if (String.IsNullOrEmpty(julkNimi))
                    {
                        cmd.Parameters.AddWithValue("@JulkaisunNimi", DBNull.Value);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@JulkaisunNimi", julkNimi);
                    }

                    if (String.IsNullOrEmpty(kustantaja))
                    {
                        cmd.Parameters.AddWithValue("@KustantajanNimi", DBNull.Value);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@KustantajanNimi", kustantaja);
                    }

                    if (String.IsNullOrEmpty(julkTunnus))
                    {
                        cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
                    }

                    cmd.Parameters.AddWithValue("@JulkaisunTilaKoodi", ODStilaKoodi);

                    cmd.Connection = conn;

                    SqlDataReader reader = cmd.ExecuteReader();

                    return reader;

                }

            }

            return null;

        }


        // Tunnistussääntö 4:
        public SqlDataReader haku_ODS_alueelta_emoJulkNimi_julkNimi(SqlConnection conn, string julkTunnus, string julkTyyppi, string emoJulkNimi, string julkNimi, int ODStilaKoodi)
        {

            if (julkTyyppi.Equals("A3") || julkTyyppi.Equals("A4") || julkTyyppi.Equals("B2") || julkTyyppi.Equals("B3") || julkTyyppi.Equals("D1") || julkTyyppi.Equals("D2") || julkTyyppi.Equals("D3") || julkTyyppi.Equals("E1"))
            {

                if (!(julkNimi.Equals("introduction")) && !(julkNimi.Equals("esipuhe")) && !(julkNimi.Equals("johdanto")))
                {

                    conn.Open();

                    SqlCommand cmd = new SqlCommand();
                    cmd.CommandType = CommandType.Text;

                    cmd.CommandText = "SELECT JulkaisunTunnus, JulkaisunOrgTunnus, OrganisaatioTunnus, JulkaisutyyppiKoodi FROM dbo.ODS_JulkaisutTMP WHERE " +
                    " EmoJulkaisunNimi = @EmoJulkaisunNimi AND JulkaisunNimi = @JulkaisunNimi AND JulkaisunTunnus <> @JulkaisunTunnus AND " +
                    "JulkaisunTilaKoodi > @JulkaisunTilaKoodi";

                    if (String.IsNullOrEmpty(emoJulkNimi))
                    {
                        cmd.Parameters.AddWithValue("@EmoJulkaisunNimi", DBNull.Value);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@EmoJulkaisunNimi", emoJulkNimi);
                    }

                    if (String.IsNullOrEmpty(julkNimi))
                    {
                        cmd.Parameters.AddWithValue("@JulkaisunNimi", DBNull.Value);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@JulkaisunNimi", julkNimi);
                    }

                    if (String.IsNullOrEmpty(julkTunnus))
                    {
                        cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
                    }

                    cmd.Parameters.AddWithValue("@JulkaisunTilaKoodi", ODStilaKoodi);

                    cmd.Connection = conn;

                    SqlDataReader reader = cmd.ExecuteReader();

                    return reader;

                }

            }

            return null;

        }


        // Tunnistussääntö 5 v.1:
        public string haku_ODS_alueelta_ISBN1_julkNimi(string server, string julkTunnus, string isbn1, string julkNimi, int ODStilaKoodi)
        {

            string connectionString_ods_julkaisut = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_ods_julkaisut);

            conn.Open();

            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.Text;

            cmd.CommandText = "SELECT TOP 1 JulkaisunTunnus FROM [dbo].[ODS_ISBN] WHERE ISBN = @ISBN AND JulkaisunTunnus IN" +
                "(SELECT JulkaisunTunnus FROM dbo.ODS_JulkaisutTMP WHERE JulkaisunNimi = @JulkaisunNimi AND JulkaisunTunnus <> @JulkaisunTunnus AND JulkaisunTilaKoodi > @JulkaisunTilaKoodi)";

            if (String.IsNullOrEmpty(isbn1))
            {
                cmd.Parameters.AddWithValue("@ISBN", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@ISBN", isbn1);
            }

            if (String.IsNullOrEmpty(julkNimi))
            {
                cmd.Parameters.AddWithValue("@JulkaisunNimi", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunNimi", julkNimi);
            }

            if (String.IsNullOrEmpty(julkTunnus))
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            }

            cmd.Parameters.AddWithValue("@JulkaisunTilaKoodi", ODStilaKoodi);

            cmd.Connection = conn;

            string tunnus = cmd.ExecuteScalar().ToString();

            conn.Close();

            return tunnus;

        }


        // Tunnistussääntö 5 v.2:
        public string haku_ODS_alueelta_ISBN2_julkNimi(string server, string julkTunnus, string isbn2, string julkNimi, int ODStilaKoodi)
        {

            string connectionString_ods_julkaisut = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_ods_julkaisut);

            conn.Open();

            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.Text;

            cmd.CommandText = "SELECT TOP 1 JulkaisunTunnus FROM [dbo].[ODS_ISBN] WHERE ISBN = @ISBN AND JulkaisunTunnus IN" +
                "(SELECT JulkaisunTunnus FROM dbo.ODS_JulkaisutTMP WHERE JulkaisunNimi = @JulkaisunNimi AND JulkaisunTunnus <> @JulkaisunTunnus AND JulkaisunTilaKoodi > @JulkaisunTilaKoodi)";

            if (String.IsNullOrEmpty(isbn2))
            {
                cmd.Parameters.AddWithValue("@ISBN", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@ISBN", isbn2);
            }

            if (String.IsNullOrEmpty(julkNimi))
            {
                cmd.Parameters.AddWithValue("@JulkaisunNimi", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunNimi", julkNimi);
            }

            if (String.IsNullOrEmpty(julkTunnus))
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            }

            cmd.Parameters.AddWithValue("@JulkaisunTilaKoodi", ODStilaKoodi);

            cmd.Connection = conn;

            string tunnus = cmd.ExecuteScalar().ToString();

            conn.Close();

            return tunnus;

        }

        // Tunnistussääntö 6:


        public SqlDataReader haku_ODS_alueelta_julkTyyppi_julkNimi_lehdenNimi_julkaisuvuosi(SqlConnection conn, string julkTyyppi, string julkNimi, string lehdenNimi, string julkTunnus, int vuosi, int ODStilaKoodi)
        {

            {


                conn.Open();

                SqlCommand cmd = new SqlCommand();
                cmd.CommandType = CommandType.Text;



                cmd.CommandText = "SELECT JulkaisunTunnus FROM dbo.ODS_JulkaisutTMP WHERE JulkaisutyyppiKoodi = @JulkaisutyyppiKoodi AND  " +
                        "JulkaisunNimi = @JulkaisunNimi AND  lehdenNimi= @LehdenNimi AND Julkaisuvuosi = @Julkaisuvuosi AND " +
                        "JulkaisunTunnus <> @JulkaisunTunnus AND JulkaisunTilaKoodi > @JulkaisunTilaKoodi";

                if (String.IsNullOrEmpty(julkTyyppi))
                {
                    cmd.Parameters.AddWithValue("@JulkaisutyyppiKoodi", DBNull.Value);
                }
                else
                {
                    cmd.Parameters.AddWithValue("@JulkaisutyyppiKoodi", julkTyyppi);
                }

                if (String.IsNullOrEmpty(julkNimi))
                {
                    cmd.Parameters.AddWithValue("@JulkaisunNimi", DBNull.Value);
                }
                else
                {
                    cmd.Parameters.AddWithValue("@JulkaisunNimi", julkNimi);
                }

                if (String.IsNullOrEmpty(lehdenNimi))
                {
                    cmd.Parameters.AddWithValue("@LehdenNimi", DBNull.Value);
                }
                else
                {
                    cmd.Parameters.AddWithValue("@LehdenNimi", lehdenNimi);
                }


                if (String.IsNullOrEmpty(julkTunnus))
                {
                    cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
                }
                else
                {
                    cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
                }

                cmd.Parameters.AddWithValue("@Julkaisuvuosi", vuosi);

                cmd.Parameters.AddWithValue("@JulkaisunTilaKoodi", ODStilaKoodi);


                cmd.Connection = conn;

                SqlDataReader reader = cmd.ExecuteReader();

                return reader;

            }
        }

                    public SqlDataReader haku_ODS_alueelta_julkTyyppi_julkNimi_kustantajanNimi_julkaisuvuosi(SqlConnection conn, string julkTyyppi, string julkNimi, string kustantaja, string julkTunnus, int vuosi, int ODStilaKoodi)
        {

            {


                conn.Open();

                SqlCommand cmd = new SqlCommand();
                cmd.CommandType = CommandType.Text;



                cmd.CommandText = "SELECT JulkaisunTunnus FROM dbo.ODS_JulkaisutTMP WHERE JulkaisutyyppiKoodi = @JulkaisutyyppiKoodi AND  " +
                        "JulkaisunNimi = @JulkaisunNimi AND  KustantajanNimi = @KustantajanNimi AND Julkaisuvuosi = @Julkaisuvuosi AND " +
                        "JulkaisunTunnus <> @JulkaisunTunnus AND JulkaisunTilaKoodi > @JulkaisunTilaKoodi";

                if (String.IsNullOrEmpty(julkTyyppi))
                {
                    cmd.Parameters.AddWithValue("@JulkaisutyyppiKoodi", DBNull.Value);
                }
                else
                {
                    cmd.Parameters.AddWithValue("@JulkaisutyyppiKoodi", julkTyyppi);
                }

                if (String.IsNullOrEmpty(julkNimi))
                {
                    cmd.Parameters.AddWithValue("@JulkaisunNimi", DBNull.Value);
                }
                else
                {
                    cmd.Parameters.AddWithValue("@JulkaisunNimi", julkNimi);
                }

                if (String.IsNullOrEmpty(kustantaja))
                {
                    cmd.Parameters.AddWithValue("@KustantajanNimi", DBNull.Value);
                }
                else
                {
                    cmd.Parameters.AddWithValue("@KustantajanNimi", kustantaja);
                }


                if (String.IsNullOrEmpty(julkTunnus))
                {
                    cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
                }
                else
                {
                    cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
                }

                cmd.Parameters.AddWithValue("@Julkaisuvuosi", vuosi);

                cmd.Parameters.AddWithValue("@JulkaisunTilaKoodi", ODStilaKoodi);


                cmd.Connection = conn;

                SqlDataReader reader = cmd.ExecuteReader();

                return reader;

            }

        }
    
                   
           
        //##

        // [julkaisut_ods].[dbo].[SA_Julkaisut]
        //
        // Haetaan tieto siita loytyyko SA_Julkaisut -taulusta parametrina annettu julkaisuntunnus, jolle tilakoodi = -1.
        // funktio palauttaa arvon true, jos loytyy ja false muuten
        public bool julkaisunTunnus_loytyy_SA_alueelta_tilakoodilla_miinus_yksi(string server, string julkTunnus)
        {

            string connectionString_ods_julkaisut = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_ods_julkaisut);

            conn.Open();

            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.Text;

            cmd.CommandText = "SELECT COUNT(*) FROM dbo.SA_Julkaisut WHERE JulkaisunTunnus = @JulkaisunTunnus AND JulkaisunTilaKoodi = -1";

            if (String.IsNullOrEmpty(julkTunnus))
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            }

            cmd.Connection = conn;

            int loytyy = (int)cmd.ExecuteScalar();

            if (loytyy > 0)
            {

                conn.Close();

                return true;
            }


            conn.Close();

            return false;  // tanne mennaan, jos SA-alueelta ei loydy parametrina annettua julkaisuntunnus, jolle tilakoodi = -1

        }



        // [julkaisut_mds].[koodi.JulkaisunTunnus]
        //
        // Haetaan yhteisjulkaisu_ID parametrina annetulla julkaisuntunnuksella
        public string hae_Yhteisjulkaisu_ID(string server, string julkTunnus)
        {

            string connectionString_mds_julkaisut = "Server=" + server + ";Database=julkaisut_mds;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_mds_julkaisut);

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            cmd.CommandText = "SELECT Yhteisjulkaisu_ID FROM koodi.JulkaisunTunnus WHERE JulkaisunTunnus = @JulkaisunTunnus";
            cmd.CommandType = CommandType.Text;

            if (String.IsNullOrEmpty(julkTunnus))
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            }

            cmd.Connection = conn;

            string id = cmd.ExecuteScalar().ToString();

            conn.Close();

            return id;

        }


        // [julkaisut_ods].[dbo].[eiDuplikaatit]
        //
        // Tarkistetaan loytyyko organisaatiolle parametrina annettu julkaisupari (ekaTunnus-tokaTunnus) eiDuplikaatit-taulusta
        public bool julkaisupari_loytyy_EiDuplikaattiTarkistusta_taulusta(string server, string organisaatio, string ekaTunnus, string tokaTunnus)
        {

            // jos jokin parametri on null, niin palautetaan false
            if ((organisaatio == null) || (ekaTunnus == null) || (tokaTunnus == null))
            {
                return false;
            }

            // jos jokin parametri on tyhja, niin palautetaan false
            if (organisaatio.Equals("") || ekaTunnus.Equals("") || tokaTunnus.Equals(""))
            {
                return false;
            }

            string connectionString_ods_julkaisut = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_ods_julkaisut);

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            cmd.CommandText = "SELECT COUNT(*) FROM dbo.EiDuplikaattiTarkistusta WHERE organisaatiotunnus = @organisaatiotunnus AND ((ekajulkorgtunnus = @ekaTunnus AND tokajulkorgtunnus = @tokaTunnus) OR (ekajulkorgtunnus = @tokaTunnus AND tokajulkorgtunnus = @ekaTunnus))";
            cmd.CommandType = CommandType.Text;

            // organisaatiotunnus
            if (String.IsNullOrEmpty(organisaatio))
            {
                cmd.Parameters.AddWithValue("@organisaatiotunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@organisaatiotunnus", organisaatio);
            }

            // ekaJulkOrgTunnus
            if (String.IsNullOrEmpty(ekaTunnus))
            {
                cmd.Parameters.AddWithValue("@ekaTunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@ekaTunnus", ekaTunnus);
            }

            // tokaJulkOrgTunnus
            if (String.IsNullOrEmpty(tokaTunnus))
            {
                cmd.Parameters.AddWithValue("@tokaTunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@tokaTunnus", tokaTunnus);
            }

            cmd.Connection = conn;

            int maara = (int)cmd.ExecuteScalar();

            conn.Close();

            if (maara > 0)
            {
                return true;
            }

            return false;

        }


        // [julkaisut_ods].[dbo].[Tarkistusloki]
        //
        // Lisataan Tarkistusloki-tauluun uusi rivi (sisainen duplikaatti tai yhteisjulkaisu)
        public void tarkistusLoki_insert_rivi(string server, string julkTunnus, string julkOrgTunnus, string orgTunnus, string loadID, int tarkID, int tilaKoodi, string kuv)
        {

            string connectionString_ods_julkaisut = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_ods_julkaisut);

            conn.Open();

            using (conn)
            {
                SqlCommand cmd = new SqlCommand("INSERT INTO Tarkistusloki (JulkaisunTunnus, JulkaisunOrgTunnus, OrganisaatioTunnus, LatausID, TarkistusID, Tila, Kuvaus) VALUES (@JulkaisunTunnus, @JulkaisunOrgTunnus, @OrganisaatioTunnus, @LatausID, @TarkistusID, @Tila, @Kuvaus)");
                cmd.CommandType = CommandType.Text;
                cmd.Connection = conn;
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);

                // JulkaisunOrgTunnus
                if (String.IsNullOrEmpty(julkOrgTunnus))
                {
                    cmd.Parameters.AddWithValue("@JulkaisunOrgTunnus", DBNull.Value);
                }
                else
                    cmd.Parameters.AddWithValue("@JulkaisunOrgTunnus", julkOrgTunnus);

                // Organisaatio
                cmd.Parameters.AddWithValue("@OrganisaatioTunnus", orgTunnus);

                // LatausID
                if (String.IsNullOrEmpty(loadID))
                {
                    cmd.Parameters.AddWithValue("@LatausID", DBNull.Value);
                }
                else
                    cmd.Parameters.AddWithValue("@LatausID", loadID);

                // TarkistusID
                cmd.Parameters.AddWithValue("@TarkistusID", tarkID);

                // JulkaisunTilaKoodi
                cmd.Parameters.AddWithValue("@Tila", tilaKoodi);

                // ODS_Julkaisut-taulun julkaisunOrgTunnus
                if (String.IsNullOrEmpty(kuv))
                {
                    cmd.Parameters.AddWithValue("@Kuvaus", DBNull.Value);
                }
                else
                    cmd.Parameters.AddWithValue("@Kuvaus", kuv);

                cmd.ExecuteNonQuery();

            }

            conn.Close();

        }


        // [julkaisut_ods].[dbo].[SA_Julkaisut]
        // [julkaisut_ods].[dbo].[ODS_Julkaisut]
        //
        // Paivitetaan SA_Julkaisut-tauluun TilaKoodi (sisaiselle duplikaatille -1 ja yhteisjulkaisulle 9)
        public void julkaisut_update_tilaKoodi(string server, int tilaKoodi, string julkTunnus, string taulu)
        {

            string connectionString_ods_julkaisut = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_ods_julkaisut);

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            using (conn)
            {

                if (taulu.Equals("SA"))
                {
                    cmd = new SqlCommand("UPDATE dbo.SA_Julkaisut SET JulkaisunTilaKoodi = @JulkaisunTilaKoodi WHERE JulkaisunTunnus = @JulkaisunTunnus");
                }
                else if (taulu.Equals("ODS"))
                {
                    cmd = new SqlCommand("UPDATE dbo.ODS_Julkaisut SET JulkaisunTilaKoodi = @JulkaisunTilaKoodi WHERE JulkaisunTunnus = @JulkaisunTunnus");
                }

                cmd.CommandType = CommandType.Text;
                cmd.Connection = conn;
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
                cmd.Parameters.AddWithValue("@JulkaisunTilaKoodi", tilaKoodi);
                cmd.ExecuteNonQuery();
            }

            conn.Close();

        }



        // [julkaisut_ods].[dbo].[EiYhteisjulkaisuTarkistusta]
        //
        // Tarkistetaan loytyyko EiYhteisjulkaisuTarkistusta-taulusta julkaisuparia. Jos loytyy, niin ei tehda tarkistusta
        public bool julkaisupari_loytyy_EiYhteisjulkaisuTarkistusta_taulusta(string server, string ekaTunnus, string tokaTunnus)
        {

            // jos jokin parametri on null, niin palautetaan false
            if ((ekaTunnus == null) || (tokaTunnus == null))
            {
                return false;
            }

            // jos jokin parametri on tyhja, niin palautetaan false
            if (ekaTunnus.Equals("") || tokaTunnus.Equals(""))
            {
                return false;
            }

            string connectionString_ods_julkaisut = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_ods_julkaisut);

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            cmd.CommandText = "SELECT COUNT(*) FROM dbo.EiYhteisjulkaisuTarkistusta WHERE ((ekajulkaisuntunnus = @ekaTunnus AND tokajulkaisuntunnus = @tokaTunnus) OR (ekajulkaisuntunnus = @tokaTunnus AND tokajulkaisuntunnus = @ekaTunnus))";
            cmd.CommandType = CommandType.Text;


            // ekajulkaisuntunnus
            if (String.IsNullOrEmpty(ekaTunnus))
            {
                cmd.Parameters.AddWithValue("@ekaTunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@ekaTunnus", ekaTunnus);
            }

            // tokaJulkOrgTunnus
            if (String.IsNullOrEmpty(tokaTunnus))
            {
                cmd.Parameters.AddWithValue("@tokaTunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@tokaTunnus", tokaTunnus);
            }

            cmd.Connection = conn;

            int maara = (int)cmd.ExecuteScalar();

            conn.Close();

            if (maara > 0)
            {
                return true;
            }

            return false;

        }




        // [julkaisut_mds].[koodi.JulkaisunTunnus]
        //
        // haetaan suurin arvo [julkaisut_mds].[koodi].[JulkaisunTunnus] -taulusta ja lisataan siihen arvo 1
        public string hae_suurin_Yhteisjulkaisu_ID(string server)
        {

            string connectionString_mds_julkaisut = "Server=" + server + ";Database=julkaisut_mds;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_mds_julkaisut);

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            // haetaan nykyinen suurin arvo ja lisataan siihen 1
            string query_suurin = "SELECT MAX(Yhteisjulkaisu_ID) + 1 AS ID FROM koodi.JulkaisunTunnus";
            cmd.Connection = conn;
            cmd.CommandText = query_suurin;

            // suurinID on suurin ID-sarakkeen arvo
            string suurinID = cmd.ExecuteScalar().ToString();

            conn.Close();

            return suurinID;

        }


        // [julkaisut_mds].[koodi.JulkaisunTunnus]
        //
        // Paivitetaan Yhteisjulkaisu_ID parametrina annetulla julkaisulle
        public void update_Yhteisjulkaisu_ID(string server, string julkTunnus, string uusi_ID)
        {

            string connectionString_mds_julkaisut = "Server=" + server + ";Database=julkaisut_mds;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_mds_julkaisut);

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            using (conn)
            {

                cmd = new SqlCommand("UPDATE koodi.JulkaisunTunnus SET Yhteisjulkaisu_ID = @Yhteisjulkaisu_ID WHERE JulkaisunTunnus = @JulkaisunTunnus");
                cmd.CommandType = CommandType.Text;
                cmd.Connection = conn;
                cmd.Parameters.AddWithValue("@Yhteisjulkaisu_ID", uusi_ID);
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
                cmd.ExecuteNonQuery();
            }

            conn.Close();

        }


        // [julkaisut_mds].[koodi.JulkaisunTunnus]
        //
        // lasketaan kuinka monta kertaa parametrina annettu organisaatio esiintyy yhteisjulkaisussa
        public bool organisaatio_loytyy_jo_yhteisjulkaisusta(string server, string organisaatio, string yhteisjulkaisu)
        {

            string connectionString_mds_julkaisut = "Server=" + server + ";Database=julkaisut_mds;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_mds_julkaisut);

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            string query = "SELECT COUNT(*) FROM koodi.JulkaisunTunnus WHERE OrgTunnus = @OrgTunnus AND Yhteisjulkaisu_ID = @Yhteisjulkaisu_ID";
            cmd.Connection = conn;
            cmd.CommandText = query;
            cmd.Parameters.AddWithValue("@OrgTunnus", organisaatio);
            cmd.Parameters.AddWithValue("@Yhteisjulkaisu_ID", yhteisjulkaisu);

            int maara = (int)cmd.ExecuteScalar();

            conn.Close();

            if (maara > 0)
            {
                return true;
            }
            else
            {
                return false;
            }

        }


        // [julkaisut_mds].[koodi].[JulkaisunTunnus]
        //
        // haetaan parametrina annetun organisaation julkaisunOrgTunnus parametrina annetulle yhteisjulkaisulle
        public SqlDataReader hae_yhteisjulkaisun_orgTunnus_duplikaatille(SqlConnection conn, string organisaatio, string yhteisjulkaisu)
        {

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            // haetaan julkaisun organisaatiotunnus
            string query = "SELECT JulkaisunTunnus, JulkaisunOrgTunnus FROM koodi.JulkaisunTunnus WHERE OrgTunnus = @OrgTunnus AND Yhteisjulkaisu_ID = @Yhteisjulkaisu_ID AND JulkaisunTila > 0";
            cmd.Connection = conn;
            cmd.CommandText = query;
            cmd.Parameters.AddWithValue("@OrgTunnus", organisaatio);
            cmd.Parameters.AddWithValue("@Yhteisjulkaisu_ID", yhteisjulkaisu);

            SqlDataReader reader = cmd.ExecuteReader();

            return reader;

        }


        // [julkaisut_mds].[koodi].[JulkaisunTunnus]
        //
        // Tarkistetaan loytyyko julkaisu jo [julkaisut_mds].[koodi].[JulkaisunTunnus]-taulusta
        public bool julkaisu_loytyy_koodi_julkaisunTunnus_taulusta(string server, string julkTunnus)
        {

            string connectionString_mds_julkaisut = "Server=" + server + ";Database=julkaisut_mds;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_mds_julkaisut);

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            cmd.CommandText = "SELECT COUNT(*) FROM koodi.JulkaisunTunnus WHERE JulkaisunTunnus = @JulkaisunTunnus";
            cmd.CommandType = CommandType.Text;
            cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            cmd.Connection = conn;

            int maara = (int)cmd.ExecuteScalar();

            conn.Close();

            if (maara > 0)
            {
                return true;
            }

            return false;

        }

    }

}