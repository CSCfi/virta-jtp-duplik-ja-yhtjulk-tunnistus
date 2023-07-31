using System;
using System.Data;

namespace Duplik_ja_yhtJulk_tunnistus
{
    
    static class Globals
    {    
        public static int min_vuosi;
        public static int tilaKoodi_vertailtava_julkaisu;
        public static int tilaKoodi_sisainen_duplikaatti;
        public static int tarkistusID_sisainen_duplikaatti;
        public static int tarkistusID_yhteisjulkaisu;
    }    

    class Program
    {

        static void Main(string[] args)
        {

            // Ajastin joka mittaa kuinka kauan koodin ajo kestaa 
            // var watch = System.Diagnostics.Stopwatch.StartNew();

            if (args.Length != 1)
            {
                Console.Write("Argumenttien määrä on väärä.");
                Environment.Exit(0);
            }

            // Palvelin
            string server = args[0];
            string ConnString = "Server=" + server + ";Trusted_Connection=true";

            // Täällä ovat tarvittavat apufunktiot ja tietokantaoperaatiot
            Apufunktiot apufunktiot = new Apufunktiot();
            Tietokantaoperaatiot tietokantaoperaatiot = new Tietokantaoperaatiot(ConnString);

            // Globaalit muuttujat
            Globals.min_vuosi = 2015; // Tarkistus ja vertailu vain niille julkaisuille, joille julkaisuvuosi >= min_vuosi.
            Globals.tilaKoodi_vertailtava_julkaisu = 1; // ODS_Julkaisut-taulun vertailtavan julkaisun tilaKoodi ei saa olla -1 tai 0. Jos on -1, niin kyseessa on epävalidi julkaisu ja jos on 0, niin kyseessa on jo aikaisemmin sisäiseksi duplikaatiksi identifioitu julkaisu       
            Globals.tilaKoodi_sisainen_duplikaatti = 0;
            Globals.tarkistusID_sisainen_duplikaatti = tietokantaoperaatiot.hae_tarkistusID("sis_dupli");
            Globals.tarkistusID_yhteisjulkaisu = tietokantaoperaatiot.hae_tarkistusID("sis_yhtj");



            // ---------------------------------------------------------------------------------------------------------------
            //
            // Ohjelman rakenne
            //
            // 1. Alustus
            // - Julkaisujen poisto taulusta ODS_julkaisutTMP
            // - ODS_julkaisutTMP taulun lukeminen muistiin ja nimikenttien muokkaus
            // - Nimikenttien kirjoitus tauluun ODS_julkaisutTMP
            // - SA_julkaisut taulun lukeminen muistiin ja nimikenttien muokkaus
            // - Nimikenttien kirjoitus tauluun SA_julkaisutTMP ja myös tauluun ODS_julkaisutTMP
            // - ISSN ja ISBN kenttien päivitys
            // - Indeksien päivitys
            //
            // 2. Tarkistus
            // - Ehdokkaiden etsintä
            // - Liputus onko duplikaatti vai yhteisjulkaisu
            //
            // 3. Toimenpiteet
            // - Duplikaattien tilan päivitys tauluun SA_Julkaisut
            // - YhteisjulkaisuID:n generointi uusille yhteisjulkaisuille
            // - YhteisjulkaisuID:n päivitys master-tauluun
            // - Kirjoitus tarkistuslokiin
            //
            // ---------------------------------------------------------------------------------------------------------------



            ////// 1. Vaihe


            string taulu1 = "julkaisut_ods.dbo.SA_JulkaisutTMP";
            string taulu2 = "julkaisut_ods.dbo.ODS_JulkaisutTMP";


            //// ODS            

            // Poistetaan vertailutaulusta sellaiset julkaisut, jotka on poistettu aiemmin sekä sellaiset, jotka ovat nyt SA-taulussa eli joiden tiedot mahdollisesti muuttuneet
            tietokantaoperaatiot.poista_poistetetut_julkaisut();

            // Luetaan ODS_Julkaisut taulusta sellaiset julkaisut, joita ei ole taulussa ODS_JulkaisutTMP, ja muokataan nimiä
            DataTable dt1 = tietokantaoperaatiot.lue_tietokantataulu_datatauluun("ODS");

            foreach (DataRow row in dt1.Rows)
            {
                row["JulkaisunNimi"] = apufunktiot.muokkaa_nimea(row["JulkaisunNimi"].ToString());
                row["KustantajanNimi"] = apufunktiot.muokkaa_nimea(row["KustantajanNimi"].ToString());
                row["EmojulkaisunNimi"] = apufunktiot.muokkaa_nimea(row["EmojulkaisunNimi"].ToString());
                row["Lehdennimi"] = apufunktiot.muokkaa_nimea(row["Lehdennimi"].ToString());
                row["DOI"] = apufunktiot.muokkaa_DOI(row["DOI"].ToString());
            }
            
            tietokantaoperaatiot.kirjoita_datataulu_tietokantaan(dt1, taulu2);           


            //// SA

            tietokantaoperaatiot.tyhjenna_taulu(taulu1);

            // SA_Julkaisut-taulun tiedot datatauluun, jota muokataan ja joka lopuksi kirjoitetaan TMP-tauluun
            DataTable dt2 = tietokantaoperaatiot.lue_tietokantataulu_datatauluun("SA");

            foreach (DataRow row in dt2.Rows)
            {               
                row["JulkaisunNimi"] = apufunktiot.muokkaa_nimea(row["JulkaisunNimi"].ToString());
                row["KustantajanNimi"] = apufunktiot.muokkaa_nimea(row["KustantajanNimi"].ToString());
                row["EmojulkaisunNimi"] = apufunktiot.muokkaa_nimea(row["EmojulkaisunNimi"].ToString());
                row["Lehdennimi"] = apufunktiot.muokkaa_nimea(row["Lehdennimi"].ToString());
                row["DOI"] = apufunktiot.muokkaa_DOI(row["DOI"].ToString());        
            }

            // Kirjoitus TMP-tauluun
            tietokantaoperaatiot.kirjoita_datataulu_tietokantaan(dt2, taulu1);

            // Kirjoitus myös ODS_JulkaisutTMP-tauluun tulevia vertailuja varten. Indeksien disablointi ennen kirjoitusta.
            string[] estettava_taulu = taulu2.Split('.');
            tietokantaoperaatiot.esta_indeksit(estettava_taulu[0], estettava_taulu[1], estettava_taulu[2]);
            tietokantaoperaatiot.kirjoita_datataulu_tietokantaan(dt2, taulu2);

            // ISSN- ja ISBN-tietojen päivitys SA_JulkaisutTMP-tauluun
            tietokantaoperaatiot.paivita_ISSN_ja_ISBN_tunnukset(taulu1);

            tietokantaoperaatiot.uudelleenrakenna_indeksit(taulu1);
            tietokantaoperaatiot.uudelleenrakenna_indeksit(taulu2);



            // ----------------------------------------------------------------------------------------------------


            ////// 2. vaihe


            /*  
                TUNNISTUS
             
                Tarkistetaan löytyykö ODS_JulkaisutTMP-taulusta sellaisia julkaisuja, jotka ovat yhteisjulkaisuja tai sisäisiä duplikaatteja SA_Julkaisut-taulun julkaisun kanssa. 
                Alla on lueteltu ehdot, jotka tarkistetaan.

                Ehto 1 (Tunnistussääntö 1): DOI - tunnus
                Ehto 2 (Tunnistussääntö 2 v1): ISSN1 + volyymi + numero + sivut + julkaisun nimi
                Ehto 3 (Tunnistussääntö 2 v2): ISSN2 + volyymi + numero + sivut + julkaisun nimi

                Edit 3.11.2016: Ehtoja 4 ja 5 ei tarvita, koska ehtoihin 2 ja 3 tehtiin muutos siten, että niissä on mukana myös julkaisun nimi, jota ei aikaisemmin niissä ollut
                (Ehto 4: ISSN1 + volyymi + numero + julkaisun nimi)
                (Ehto 5: ISSN2 + volyymi + numero + julkaisun nimi)

                Ehto 6 (Tunnistussääntö 3): julkaisutyyppi + kustantaja + julkaisun nimi(koskee julkaisutyyppeja C1, D5, E1, E2 pl.Introduction, Esipuhe, Johdanto)
                Ehto 7 (Tunnistussääntö 4): emojulkaisun nimi +julkaisun nimi(koskee julkaisutyyppeja A3, A4, B2, B3, D1, D2, D3, E1, pl.Introduction, Esipuhe, Johdanto)
                Ehto 8 (Tunnistussääntö 5 v1): ISBN1 + julkaisun nimi
                Ehto 9 (Tunnistussääntö 5 v2): ISBN2 + julkaisun nimi

                2 / 2022
                Ehto 10 (Tunnistussääntö 6): Julkaisutyyppi + julkaisun nimi + lehden nimi + julkaisuvuosi(koskee julkaisutyyppeja D1)
                Ehto 11 (Tunnistussääntö 7): Julkaisutyyppi + julkaisun nimi + kustantajan nimi + julkaisuvuosi(koskee julkaisutyyppeja D4)

                
                RAJOITTEET
                Tunnistussäännöt 1 - 5
                    - julkaisut_ods.dbo.EiYhteisjulkaisuJulkaisutyyppiparit
                Tunnistussäännöt 1 - 7
                    - julkaisut_ods.dbo.EiDuplikaattiTarkistusta
                Tunnistussäännöt 2 - 5
                    - DOIt eivät saa olla erisuuria
                
                
                LIPUTUS
             
                Sisäinen duplikaatti: 
                1. Duplikaatin Yhteisjulkaisu_ID = 0, sama organisaatio, eri julkaisunOrgTunnus
                2. Duplikaatin Yhteisjulkaisu_ID <> 0, organisaatio on mukana yhteisjulkaisussa, julkaisunOrgTunnus on eri

                Yhteisjulkaisu:
                1. Duplikaatin Yhteisjulkaisu_ID = 0, eri organisaatio
                2. Duplikaatin Yhteisjulkaisu_ID <> 0, organisaatio ei mukana yhteisjulkaisussa

            */


            //tietokantaoperaatiot.uudelleenjarjesta_indeksit("julkaisut_mds.koodi.julkaisuntunnus");


            /* 
                Silmukka, jossa julkaisuille etsitään potentiaalinen duplikaatti/yhteisjulkaisu ja katsotaan täyttääkö ehdokas yllä olevat ehdot.
                Jos ehdokas ei täytä ehtoja niin julkaisu on mukana seuraavalla kierroksella.
            */

            int ehdot_lkm = 11;
            for (int i = 1; i <= ehdot_lkm; i++)
            {
                if (i == 4 || i == 5) { continue; }
                tietokantaoperaatiot.etsi_yhteisjulkaisut(i);
                tietokantaoperaatiot.liputa_duplikaatit(i);
                tietokantaoperaatiot.liputa_yhteisjulkaisut(i);
            }

            // Muutosten jälkeen taas indeksien päivitys
            tietokantaoperaatiot.uudelleenjarjesta_indeksit(taulu1);



            // ----------------------------------------------------------------------------------------------------


            ////// 3. vaihe


            /*
                Sisäinen duplikaatti: 
                -> Julkaisun tilan päivitys tauluun julkaisut_ods.dbo.SA_Julkaisut ja kirjoitus tarkistuslokiin

                Yhteisjulkaisu:
                - Jos yhteisjulkaisua ei ole vielä olemassa
                -> Yhteisjulkaisu ID:n generointi ja päivitys molemmille julkaisuille tauluun julkaisut_mds.koodi.julkaisuntunnus sekä kirjoitus tarkistuslokiin
                - Jos yhteisjulkaisu on olemassa
                -> Yhteisjulkaisu ID:n päivitys tauluun julkaisut_mds.koodi.julkaisuntunnus ja kirjoitus tarkistuslokiin
            */


            // Jos duplikaatti
            tietokantaoperaatiot.paivita_duplikaatit();

            // Jos yhteisjulkaisu
            tietokantaoperaatiot.luo_yhteisjulkaisut();
            tietokantaoperaatiot.paivita_yhteisjulkaisut();

            // Tarkistuslokiin
            tietokantaoperaatiot.kirjoita_tarkistuslokiin();



            // ----------------------------------------------------------------------------------------------------

            loppu:

            //Console.Write("Loppu");
            //Console.ReadLine();
            ;
        }

        // Ajastin, loppu
        //watch.Stop();
        //var elapsedMs = watch.ElapsedMilliseconds;
        //Console.Write("Koko ajon kesto: " + elapsedMs);


    }

}