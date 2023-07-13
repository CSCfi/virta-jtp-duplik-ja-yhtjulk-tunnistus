using System;
using System.Data;


namespace Duplik_ja_yhtJulk_tunnistus
{
    class Apufunktiot
    {

        // nama stop wordsit hypataan yli eli naita ei oteta huomioon pitkissa stringeissa (julkaisun nimi, kustantaja jne.)
        private string[] stop_words = {" i "," me "," my "," myself "," we "," our "," ours "," ourselves "," you "," your "," yours "," yourself "," yourselves "," he "," him "," his "," himself "," she "," her "," hers "," herself "," it "," its "," itself "," they "," them "," their "," theirs ",
" themselves "," what "," which "," who "," whom "," this "," that "," these "," those "," am "," is "," are "," was "," were "," be "," been "," being "," have "," has "," had "," having "," do "," does "," did "," doing "," a "," an "," the "," and "," but "," if "," or "," because "," as "," until ",
" while "," of "," at "," by "," for "," with "," about "," against "," between "," into "," through "," during "," before "," after "," above "," below "," to "," from "," up "," down "," in "," out "," on "," off "," over "," under "," again "," further "," then "," once "," here "," there ",
" when "," where "," why "," how "," all "," any "," both "," each "," few "," more "," most "," other "," some "," such "," no "," nor "," not "," only "," own "," same "," so "," than "," too "," very "," s "," t "," can "," will "," just "," don "," should "," now "};

        // nama stop charsit hypataan yli eli naita ei oteta mukaan pitkissa stringeissa (julkaisun nimi, kustantaja jne.)
        private string[] stop_chars = { "!", "\"", "#", "$", "%", "&", "'", "(", ")", "*", "+", ",", "-", ".", "/", ":", ";", "<", "=", ">", "?", "@", "[", "\\", "]", "^", "_", "`", "{", "|", "}", "~", "£", "¿", 
                                        "®", "¬", "½", "¼", "«", "»", "©", "┐", "└", "┴", "┬", "├", "─", "┼", "┘", "┌", "¦", "¯", "´", "≡", "±", "‗", "¾", "¶", "§", "÷", "¸", "°", "¨", "·", "¹", "³", "²", "–"};

        Tietokantaoperaatiot tietokantaoperaatiot = new Tietokantaoperaatiot();

        // Muokataan parametrina annettua nimea siten, etta nimesta poistetaan stop wordsit ja stop charsit.
        // Lisaksi alusta poistetaan the, a ja an -merkit ja merkkijono trimmataan.
        // Palautetaan muokattu merkkijono.
        public string muokkaa_nimea(string nimi)
        {
            if (nimi == null || nimi.Equals(""))
            {
                return null;
            }

            // Muutetaan nimi LowerCase:ksi ja trimmataan
            nimi = nimi.ToLower().Trim();

            // Kaydaan lapi stop_chars -merkit ja poistetaan merkki mikali se loytyy nimesta
            foreach (string c in stop_chars)
            {
                if (nimi.Contains(c))
                {
                    nimi = nimi.Replace(c, " ");
                }
            }

            // Trimmataan taas nimi
            nimi = nimi.Trim();

            // Kaydaan lapi stop_words -sanat ja poistetaan sana mikali se loytyy nimesta
            foreach (string item in stop_words)
            {
                if (nimi.Contains(item))
                {
                    nimi = nimi.Replace(item, " ");
                }
            }

            // poistetaan tyhjat valimerkit
            nimi = nimi.Replace("     ", " ");
            nimi = nimi.Replace("    ", " ");
            nimi = nimi.Replace("   ", " ");
            nimi = nimi.Replace("  ", " ");

            // Jalleen trimmataan nimi
            nimi = nimi.Trim();

            // Poistetaan sitten nimen alusta sanat the, a ja an
            string sana = "";
            for (int i = 1; i < Math.Min(nimi.Length, 5); i++)
            {
                if (nimi[i] == ' ')
                {
                    sana = nimi.Substring(0, i + 1);
                    if (sana.Equals("the ") || sana.Equals("a ") || sana.Equals("an "))
                    {
                        //Console.WriteLine(nimi + "|" + nimi.Substring(i + 1));
                        nimi = nimi.Substring(i + 1);
                    }
                    break;
                }
            }


            return nimi;

        }


        public string muokkaa_DOI(string doi)
        {

            // Muokataan DOI-tunnusta, mikali se ei ole null ja pituus on yli 2. 
            // Muokkauksella halutaan, etta DOI-tunnus alkaa oikeassa muodossa eli etta ensimmainen merkki on 1 ja toinen 0.
            if (doi == null || doi.Equals(""))
            {
                return null;
            }
            
            if (doi.Length <= 2)
            {
                return doi;
            }

            // trimmataan aluksi doi
            string newDOI = doi.Trim();

            // poistetaan sitten alusta merkkeja siihen asti kunnes kaksi ensimmaista merkkia ovat 10
            int pituus = newDOI.Length;

            bool loopContinues = true;

            if (pituus <= 2)
            {
                loopContinues = false;
            }

            while (loopContinues)
            {

                char ekaMerkki = newDOI[0];
                char tokaMerkki = newDOI[1];

                // doi alkaa oikein, siis ekat kaksi merkkia ovat 10
                if ((ekaMerkki == '1') && (tokaMerkki == '0'))
                {
                    return newDOI;
                }

                else
                {
                    newDOI = newDOI.Substring(1);
                }

                pituus = newDOI.Length;

                if (pituus <= 2)
                {
                    loopContinues = false;
                }

            }

            return newDOI;

        }


        public static DataTable MakeDataTable()
        {
            // Create a new DataTable.
            DataTable table = new DataTable("TMP_Table");

            // Declare variables for DataColumn and DataRow objects.
            DataColumn column;

            // Create new DataColumn, set DataType, ColumnName and add to DataTable.
            // 1
            column = new DataColumn("JulkaisunTunnus");
            column.Caption = "JulkaisunTunnus";
            column.DataType = System.Type.GetType("System.String");
            table.Columns.Add(column);

            // 2
            column = new DataColumn("JulkaisunNimi");
            column.Caption = "JulkaisunNimi";
            column.DataType = Type.GetType("System.String");
            table.Columns.Add(column);

            // 3
            column = new DataColumn("KustantajanNimi");
            column.Caption = "KustantajanNimi";
            column.DataType = Type.GetType("System.String");
            table.Columns.Add(column);

            // 4
            column = new DataColumn("EmojulkaisunNimi");
            column.Caption = "EmojulkaisunNimi";
            column.DataType = Type.GetType("System.String");
            table.Columns.Add(column);

            // 5
            column = new DataColumn("DOI");
            column.Caption = "DOI";
            column.DataType = Type.GetType("System.String");
            table.Columns.Add(column);

            // 6
            column = new DataColumn("LehdenNimi");
            column.Caption = "Lehdennimi";
            column.DataType = Type.GetType("System.String");
            table.Columns.Add(column);

            // 7
            column = new DataColumn("OrganisaatioTunnus");
            column.Caption = "OrganisaatioTunnus";
            column.DataType = System.Type.GetType("System.String");
            table.Columns.Add(column);

            // 8
            column = new DataColumn("JulkaisunOrgTunnus");
            column.Caption = "JulkaisunOrgTunnus";
            column.DataType = System.Type.GetType("System.String");
            table.Columns.Add(column);

            // 9
            column = new DataColumn("Lataus_ID");
            column.Caption = "Lataus_ID";
            column.DataType = System.Type.GetType("System.String");
            table.Columns.Add(column);

            // 10
            column = new DataColumn("JulkaisunTilaKoodi");
            column.Caption = "JulkaisunTilaKoodi";
            column.DataType = System.Type.GetType("System.String");
            table.Columns.Add(column);

            // 11
            column = new DataColumn("JulkaisutyyppiKoodi");
            column.Caption = "JulkaisutyyppiKoodi";
            column.DataType = System.Type.GetType("System.String");
            table.Columns.Add(column);

            // 12
            column = new DataColumn("SivunumeroTeksti");
            column.Caption = "SivunumeroTeksti";
            column.DataType = System.Type.GetType("System.String");
            table.Columns.Add(column);

            // 13
            column = new DataColumn("VolyymiTeksti");
            column.Caption = "VolyymiTeksti";
            column.DataType = System.Type.GetType("System.String");
            table.Columns.Add(column);

            // 14
            column = new DataColumn("LehdenNumeroTeksti");
            column.Caption = "LehdenNumeroTeksti";
            column.DataType = System.Type.GetType("System.String");
            table.Columns.Add(column);

            return table;
        }


        public void tulosta_datataulu_konsoliin(DataTable dt)
        {
            foreach (DataRow dr in dt.Rows)
            {
                foreach (var item in dr.ItemArray)
                {
                    Console.WriteLine(item + "|");
                    //if (item.ToString() == "0330267521") { Console.ReadKey(); }
                }
                Console.WriteLine();
                Console.WriteLine("------uusi------");
            }
        }

    }

}