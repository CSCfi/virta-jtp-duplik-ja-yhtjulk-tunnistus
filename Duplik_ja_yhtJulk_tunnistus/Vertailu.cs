using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Duplik_ja_yhtJulk_tunnistus
{
    class Vertailu
    {

        ////////////////////////////////////////
        //tasta alkavat sarakevertailu-metodit// 
        ////////////////////////////////////////
        //tasta alkavat sarakeVertailu-metodit//
        ////////////////////////////////////////
        //tasta alkavat sarakeVertailu-metodit//
        ////////////////////////////////////////
        //tasta alkavat sarakeVertailu-metodit//
        ////////////////////////////////////////

        ////////////////////////////////////////
        //seuraavat metodit on toteutettu://////
        ////////////////////////////////////////
        //bool samaOrganisaatio
        //bool samaJulkaisutyyppi
        //bool samaJulkaisuvuosi
        //bool samaJulkaisunNimi
        //bool samaVolyymi
        //bool samaNumero
        //bool samatSivut
        //bool samaArtikkelinumero
        //bool samaISSN
        //bool samaISBN
        //bool samaKustantaja
        //bool samaDOI
        ////////////////////////////////////////

        ////////////////////////////////////////
        //lisäksi seuraavat apumetodit://///////
        ////////////////////////////////////////
        //string replaceAtIndex
        ////////////////////////////////////////


        ////////////////////////////////////////////////////
        //tutkitaan, onko julkaisuilla sama organisaatio////
        ////////////////////////////////////////////////////
        public bool samaOrganisaatio(string ekaOrganisaatio, string tokaOrganisaatio)
        {

            if ((ekaOrganisaatio == null) || (tokaOrganisaatio == null))
            {
                return false;
            }

            // trimmataan parametrit
            ekaOrganisaatio = ekaOrganisaatio.Trim();
            tokaOrganisaatio = tokaOrganisaatio.Trim();

            if (!(ekaOrganisaatio.Equals(tokaOrganisaatio)))
            {
                // parametrit ovat valideja, mutta eivat samoja
                return false;
            }

            return true;
        }


        ////////////////////////////////////////////////////
        //tutkitaan, onko julkaisuilla sama julkaisutyyppi//
        ////////////////////////////////////////////////////
        public bool samaJulkaisutyyppi(string ekaTyyppi, string tokaTyyppi)
        {

            if ((ekaTyyppi == null) || (tokaTyyppi == null))
            {
                return false;
            }

            // muutetaan parametrit isoiksi kirjaimiksi ja trimmataan ne
            ekaTyyppi = ekaTyyppi.ToUpper().Trim();
            tokaTyyppi = tokaTyyppi.ToUpper().Trim();

            // tutkitaanko, ovatko annetut parametrit valideja
            //
            if (!ekaTyyppi.Equals("A1") && !ekaTyyppi.Equals("A2") && !ekaTyyppi.Equals("A3") &&
                    !ekaTyyppi.Equals("A4") && !ekaTyyppi.Equals("B1") && !ekaTyyppi.Equals("B1") &&
                    !ekaTyyppi.Equals("B3") && !ekaTyyppi.Equals("C1") && !ekaTyyppi.Equals("C2") &&
                    !ekaTyyppi.Equals("D1") && !ekaTyyppi.Equals("D2") && !ekaTyyppi.Equals("D3") &&
                    !ekaTyyppi.Equals("D4") && !ekaTyyppi.Equals("D5") && !ekaTyyppi.Equals("E1") &&
                    !ekaTyyppi.Equals("E2") && !ekaTyyppi.Equals("G4") && !ekaTyyppi.Equals("G5"))
            {

                return false;

            }
            else if (!tokaTyyppi.Equals("A1") && !tokaTyyppi.Equals("A2") && !tokaTyyppi.Equals("A3") &&
                  !tokaTyyppi.Equals("A4") && !tokaTyyppi.Equals("B1") && !tokaTyyppi.Equals("B2") &&
                  !tokaTyyppi.Equals("B3") && !tokaTyyppi.Equals("C1") && !tokaTyyppi.Equals("C2") &&
                  !tokaTyyppi.Equals("D1") && !tokaTyyppi.Equals("D2") && !tokaTyyppi.Equals("D3") &&
                  !tokaTyyppi.Equals("D4") && !tokaTyyppi.Equals("D5") && !tokaTyyppi.Equals("E1") &&
                  !tokaTyyppi.Equals("E2") && !tokaTyyppi.Equals("G4") && !tokaTyyppi.Equals("G5"))
            {

                return false;
            }

            if (!ekaTyyppi.Equals(tokaTyyppi))
            {
                // parametrit ovat valideja, mutta eivat samoja
                return false;
            }

            return true;
        }


        ///////////////////////////////////////////////////
        //tutkitaan, onko julkaisuilla sama julkaisuvuosi//
        ///////////////////////////////////////////////////
        public bool samaJulkaisuvuosi(int ekaVuosi, int tokaVuosi)
        {

            if (ekaVuosi != tokaVuosi)
            {
                // kyseessa ovat eri vuoden julkaisut
                return false;
            }

            // kyseessa ovat saman vuoden julkaisut
            return true;
        }


        ////////////////////////////////////////////////////
        //tutkitaan, onko julkaisuilla sama julkaisun nimi//
        ////////////////////////////////////////////////////
        public bool samaJulkaisunNimi(string ekaJulkaisunNimi, string tokaJulkaisunNimi)
        {

            if (ekaJulkaisunNimi == null || ekaJulkaisunNimi.Equals(""))
            {
                // palautetaan false, jos parametri on null tai tyhja
                return false;

            }
            else if (tokaJulkaisunNimi == null || tokaJulkaisunNimi.Equals(""))
            {
                // palautetaan false, jos parametri on null tai tyhja
                return false;
            }

            // trimmataan parametrit
            ekaJulkaisunNimi = ekaJulkaisunNimi.Trim();
            tokaJulkaisunNimi = tokaJulkaisunNimi.Trim();

            if (!ekaJulkaisunNimi.Equals(tokaJulkaisunNimi))
            {
                return false;
            }

            return true;
        }


        /////////////////////////////////////////////
        //tutkitaan, onko julkaisuilla sama volyymi//
        /////////////////////////////////////////////
        public bool samaVolyymi(string ekaVolyymi, string tokaVolyymi)
        {

            if (ekaVolyymi == null || ekaVolyymi.Equals(""))
            {
                // palautetaan false, jos parametri on null tai tyhja
                return false;

            }
            else if (tokaVolyymi == null || tokaVolyymi.Equals(""))
            {
                // palautetaan false, jos parametri on null tai tyhja
                return false;
            }

            // trimmataan parametrit
            ekaVolyymi = ekaVolyymi.Trim();
            tokaVolyymi = tokaVolyymi.Trim();

            if (!ekaVolyymi.Equals(tokaVolyymi))
            {
                return false;
            }

            return true;
        }


        ////////////////////////////////////////////
        //tutkitaan, onko julkaisuilla sama numero//
        ////////////////////////////////////////////
        public bool samaNumero(string ekaNumero, string tokaNumero)
        {

            if (ekaNumero == null || ekaNumero.Equals(""))
            {
                // palautetaan false, jos parametri on null tai tyhja
                return false;

            }
            else if (tokaNumero == null || tokaNumero.Equals(""))
            {
                // palautetaan false, jos parametri on null tai tyhja
                return false;
            }

            // trimmataan parametrit
            ekaNumero = ekaNumero.Trim();
            tokaNumero = tokaNumero.Trim();

            if (!ekaNumero.Equals(tokaNumero))
            {
                return false;
            }

            return true;
        }


        //////////////////////////////////////////////////
        //tutkitaan, onko julkaisuilla samat sivunumerot//
        //////////////////////////////////////////////////
        public bool samatSivut(string ekatSivut, string tokatSivut)
        {

            if (ekatSivut == null || ekatSivut.Equals(""))
            {
                // palautetaan false, jos parametri on null tai tyhja
                return false;

            }
            else if (tokatSivut == null || tokatSivut.Equals(""))
            {
                // palautetaan false, jos parametri on null tai tyhja
                return false;
            }

            // trimmataan parametrit
            ekatSivut = ekatSivut.Trim();
            tokatSivut = tokatSivut.Trim();

            if (!ekatSivut.Equals(tokatSivut))
            {
                return false;
            }

            return true;
        }


        /////////////////////////////////////////////////////
        //tutkitaan, onko julkaisuilla sama artikkelinumero//
        /////////////////////////////////////////////////////
        public bool samaArtikkelinumero(int ekaArtikkelinumero, int tokaArtikkelinumero)
        {


            if (ekaArtikkelinumero != tokaArtikkelinumero)
            {
                return false;
            }

            return true;
        }


        /////////////////////////////////////////////////
        //tutkitaan, onko julkaisuilla sama ISSN-numero//
        /////////////////////////////////////////////////
        public bool samaISSN(string ekaISSN, string tokaISSN)
        {

            if ((ekaISSN == null) || (tokaISSN == null))
            {
                // kyseessä on null-arvo
                return false;
            }


            if (ekaISSN.Length != 9 || tokaISSN.Length != 9)
            {
                // vaarantyyppinen ISSN
                return false;
            }


            // trimmataan parametrit
            ekaISSN = ekaISSN.Trim();
            tokaISSN = tokaISSN.Trim();

            // jaetaan ISSN-tunnukset väliviivan vasempaan ja oikeaan osaan ja jätetään välimerkki pois,
            // koska joissain tapauksissa se saattaa olla eri kuin yksinkertainen väliviiva. Yhdistetään sitten 
            // vasen ja oikea osa ja verrataan niista muodostettuja merkkijonoja keskenään.
            string ekaISSNValiPoistettuna = ekaISSN.Substring(0, 4) + ekaISSN.Substring(5, 4);
            string tokaISSNValiPoistettuna = tokaISSN.Substring(0, 4) + tokaISSN.Substring(5, 4);


            if (!ekaISSNValiPoistettuna.Equals(tokaISSNValiPoistettuna))
            {
                return false;
            }

            return true;
        }


        /////////////////////////////////////////////////
        //tutkitaan, onko julkaisuilla sama ISBN-numero//
        /////////////////////////////////////////////////
        public bool samaISBN(string ekaISBN, string tokaISBN)
        {

            // tehdaan testit, jotta ei oteta mukaan tyhjia tai null-arvoja
            if (ekaISBN == null || ekaISBN.Equals(""))
            {
                // tyhja merkkijono tai null
                return false;

            }
            else if (tokaISBN == null || tokaISBN.Equals(""))
            {
                // tyhja merkkijono tai null
                return false;
            }


            // korvataan valiviivat tyhjilla merkeilla, jolloin ISBN-tunnus on
            // pelkka numerosarja. 
            //
            // Esimerkiksi 
            // 
            // 978-952-5031-74-4 => 9789525031744
            //
            // Lisäksi trimmataan merkkijonot
            ekaISBN = ekaISBN.Replace("-", "").Trim();
            tokaISBN = tokaISBN.Replace("-", "").Trim();

            // datassa saattaa olla virheellisia ISBN-tunnuksia, joissa viimeiset
            // luvut ovat nollia. Poistetaan siis sellaiset luvut, joisssa viimeiset
            // seitsaman arvoa ovat nollia.
            //
            // Esimerkiksi 9789090000000 ei ole validi ISBN-tunnus, mutta se esiintyy
            // julkaisudatassa. Tallaiset poistetaan, koska tallainen samanlainen
            // virheellinen ISBN-tunnus voi olla usealla julkaisulla, joilla ei
            // todellisuudessa ole mitaan yhteista
            //
            // esim. alla olevilla ISBN-tunnus on 9789090000000
            //
            // EU tax law - direct taxation
            //
            // vs.
            //
            // European Court of Justice Legal Reasoning in Context

            if (ekaISBN.EndsWith("0000000") || tokaISBN.EndsWith("0000000"))
            {
                return false;
            }


            if (!ekaISBN.Equals(tokaISBN))
            {
                // ISBN-tunnukset ovat validit, mutta eivät yhtenevät
                return false;
            }

            return true;
        }


        ////////////////////////////////////////////////
        //tutkitaan, onko julkaisuilla sama kustantaja//
        ////////////////////////////////////////////////
        public bool samaKustantaja(string ekaKustantaja, string tokaKustantaja)
        {

            // tehdaan testit, jotta ei oteta mukaan tyhjia tai null-arvoja
            if (ekaKustantaja == null || ekaKustantaja.Equals(""))
            {
                // tyhja merkkijono tai null
                return false;

            }
            else if (tokaKustantaja == null || tokaKustantaja.Equals(""))
            {
                // tyhja merkkijono tai null
                return false;
            }


            // trimmataan parametrit
            ekaKustantaja = ekaKustantaja.Trim();
            tokaKustantaja = tokaKustantaja.Trim();

            if (!ekaKustantaja.Equals(tokaKustantaja))
            {
                return false;
            }

            return true;

        }


        ///////////////////////////////////////////////////////
        //tutkitaan, onko julkaisuilla sama emojulkaisun nimi//
        ///////////////////////////////////////////////////////
        public bool samaEmojulkaisunNimi(string ekaEmojulkaisunNimi, string tokaEmojulkaisunNimi)
        {

            // tehdaan testit, jotta ei oteta mukaan tyhjia tai null-arvoja
            if (ekaEmojulkaisunNimi == null || ekaEmojulkaisunNimi.Equals(""))
            {
                // tyhja merkkijono tai null
                return false;

            }
            else if (tokaEmojulkaisunNimi == null || tokaEmojulkaisunNimi.Equals(""))
            {
                // tyhja merkkijono tai null
                return false;
            }


            // trimmataan parametrit
            ekaEmojulkaisunNimi = ekaEmojulkaisunNimi.Trim();
            tokaEmojulkaisunNimi = tokaEmojulkaisunNimi.Trim();

            if (!ekaEmojulkaisunNimi.Equals(tokaEmojulkaisunNimi))
            {
                return false;
            }

            return true;

        }

        ////////////////////////////////////////////////
        //tutkitaan, onko julkaisuilla sama DOI-tunnus//
        ////////////////////////////////////////////////
        public bool samaDOI(string ekaDOI, string tokaDOI)
        {

            if (ekaDOI == null || (ekaDOI.Length < 7))
            {
                return false;
            }
            else if (tokaDOI == null || (tokaDOI.Length < 7))
            {
                return false;
            }

            // Joskus itse DOI-tunnuksen edessä on http-linkki tai
            // muita merkkejä, jotka eivät kuulu tunnukseen.
            // DOI-tunnus on muotoa 10.xxx, joten siivotaan merkkejä
            // pois niin kauan, kunnes ensimmäinen merkki 1
            int ekaPituus = ekaDOI.Length;
            int tokaPituus = tokaDOI.Length;

            if (ekaPituus < 7 || tokaPituus < 7)
            {
                // DOI-on vääränmerkkinen (liian lyhyt)
                return false;
            }

            // Siivotaan vääränlaiset etumerkit pois, jolloin jäljelle jää
            // merkkijono, jonka ensimmäinen merkki on 1.
            for (int i = 0; i < (ekaPituus - 7); i++)
            {
                char c = ekaDOI[i];
                if (c != '1')
                {
                    ekaDOI = ReplaceAtIndex(i, ' ', ekaDOI);
                }
            }

            for (int j = 0; j < (tokaPituus - 7); j++)
            {
                char c = tokaDOI[j];
                if (c != '1')
                {
                    tokaDOI = ReplaceAtIndex(j, ' ', tokaDOI);
                }
            }

            ekaDOI = ekaDOI.Trim();
            tokaDOI = tokaDOI.Trim();

            // testataan ovatko DOI-tunnukset samat
            if (!ekaDOI.Equals(tokaDOI))
            {
                return false;
            }

            return true;

        }

        ///////////////////////////////////////////////////
        //tästä alkavat apumetodit/////////////////////////
        ///////////////////////////////////////////////////
        //tästä alkavat apumetodit/////////////////////////
        ///////////////////////////////////////////////////
        //tästä alkavat apumetodit/////////////////////////
        ///////////////////////////////////////////////////
        //tästä alkavat apumetodit/////////////////////////
        ///////////////////////////////////////////////////

        // tämä on apumetodi DOI-tunnuksen käsittelyyn
        static string ReplaceAtIndex(int i, char value, string word)
        {
            char[] letters = word.ToCharArray();
            letters[i] = value;
            return string.Join("", letters);
        }

    }
}