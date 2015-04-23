--ghc -O2 CsvParser.hs && copy /y CsvParser.exe "C:\Users\Kevin\Desktop\Talent2 Data Import"

import System.Environment
import System.IO
import Data.Ord
import Data.List
import Data.Char
import Data.Time
import Data.Maybe
import Control.Monad.State.Strict
import Control.Monad.Reader

import Text.Parsec
import Text.Parsec.String  (Parser)
import Data.Either         (either)
import Control.Applicative ((<$>),(*>),(<*),(<*>),pure)


--csv line parser
------------------------------------------------------------------
parsec_unquotedcell :: (Char,Char) -> Parser String
parsec_unquotedcell (cs,ds) = many (noneOf [cs])

parsec_quotedcell :: (Char,Char) -> Parser String
parsec_quotedcell (cs,ds) = char ds *> many (noneOf [ds] <|> try (string [ds,ds] >> return ds)) <* char ds

parsec_line :: (Char,Char) -> Parser [String]
parsec_line (cs,ds) = sepBy (parsec_quotedcell (cs,ds) <|> parsec_unquotedcell (cs,ds)) (char cs)

parseCsv :: (Char,Char) -> String -> [String]
parseCsv (cs,ds) = either (const []) id . parse (parsec_line (cs,ds)) ""
------------------------------------------------------------------
    

--helpers
------------------------------------------------------------------
splitStr :: Eq a => [a] -> [a] -> [[a]]
splitStr sub str = split' sub str [] []
    where
    split' _   []  subacc acc = reverse (reverse subacc : acc)
    split' sub str subacc acc
        | sub `isPrefixOf` str = split' sub (drop (length sub) str) [] (reverse subacc:acc)
        | otherwise            = split' sub (tail str) (head str:subacc) acc


longestCol :: [String] -> Int
longestCol l = if length l > 0
    then length . maximumBy (comparing length) $ l
    else 0
------------------------------------------------------------------    


--outputs
------------------------------------------------------------------
outputCsv :: (Char,Char) -> [String] -> String
outputCsv (cs,ds) = intercalate [cs] . map (\x -> [ds] ++ (outputCol ds x) ++ [ds])
    where outputCol ds = foldr (\a b -> if a == ds then ds:ds:b else a:b) []


outputCreateTable :: String -> [String] -> String
outputCreateTable tbl x  = dropTable ++ "CREATE TABLE "++tbl++" (\n" ++ (cols x) ++ ")"
    where cols       = intercalate ",\n" . map (\x -> "[" ++ (cleanup x) ++ "] NVARCHAR(" ++ (idcol x) ++ ") NULL")
          idcol c    = if isInfixOf "UniqueID" c then "255" else "MAX"
          cleanup    = replaceSpc . printables . trim
          replaceSpc = foldr (\a b -> if a==' ' then '_':b else a:b) []
          printables = filter (\x -> isAlphaNum x || isSeparator x)
          trim       = unwords . words
          dropTable  = "IF OBJECT_ID('"++tbl++"','U') IS NOT NULL DROP TABLE "++tbl++"\n"


outputSQL :: String -> [String] -> String
outputSQL tbl x = "INSERT INTO "++tbl++" VALUES (" ++ (cols x) ++ ")"
    where cols = intercalate "," . map (\x -> "\'" ++ (quote x) ++ "\'")
          quote = foldr (\a b -> if a == '\'' then '\'':'\'':b else a:b) []


printState :: ProcState -> String
printState state = "--Processed line: " ++ show (totallines state) ++ "\n"
                   ++ "--Longest column size: " ++  show (longcolsize state)
                   ++ " on line " ++ show (longcolline state) ++ "\n"
                   ++ "--Last line: " ++ (if null (prevline state) then "Empty" else (prevline state))
------------------------------------------------------------------                


--IO stdin/file
------------------------------------------------------------------
getInputFileContent :: [String] -> IO String
getInputFileContent args =
    let getContents f = hSetNewlineMode f universalNewlineMode >> hGetContents f in
    if "-d" `elem` args
        then getContents stdin
        else case filter (\x -> head x /= '-') args of
            []    -> getContents stdin
            (f:_) -> do
                fileHandle <- openFile f ReadMode
                hSetBuffering fileHandle . BlockBuffering . Just $ 1024*10
                hSetEncoding fileHandle utf8
                getContents fileHandle
------------------------------------------------------------------                


--initial environment checks
------------------------------------------------------------------
checkCsv :: String -> (Char,Char)
checkCsv str = fst . last $ sortBy (comparing snd) (procLength)
    where testenv = [ Env { csv = (',','"'), out = (',','"'), maxcolen = 0,
                        sql = False, sqltable = "", colcount = 0, debug = False }
                    , Env { csv = ('|','~'), out = ('|','~'), maxcolen = 0,
                        sql = False, sqltable = "", colcount = 0, debug = False }
                    , Env { csv = ('|','"'), out = ('|','~'), maxcolen = 0,
                        sql = False, sqltable = "", colcount = 0, debug = False }
                    ]
          procLength = map (\envs ->
                            (csv envs,
                            (length $ parseCsv (csv envs) str) +
                            (length $ filter (==snd (csv envs)) str) ))
                            testenv
------------------------------------------------------------------                            


-- main loop using State - Reader - IO
----------------------------------------------------------------------------
doProcess :: [String] -> StateT ProcState (ReaderT Env IO) ProcState

doProcess [] = do
    ------- check if prevline can be parsed correctly
    env <- ask
    state <- get
    case parse (parsec_line (csv env)) "" (prevline state) of
        Right prevcols -> do
            if (length prevcols == colcount env) 
                then if (all (\x -> length x <= (maxcolen env)) prevcols)
                    then do
                        liftIO . hPutStrLn stdout $ outputCsv (out env) prevcols
                        let longcol = longestCol prevcols
                        if longcol > (longcolsize state)
                            then put state { longcolline = (totallines state), longcolsize = longcol, prevline = "" }
                            else put state { prevline = "" }
                    else do
                        liftIO . hPutStrLn stderr $ outputSQL (sqltable env) prevcols
                        put state { prevline = "" }
                else return ()
        Left _ -> return ()
    get >>= return

doProcess (x:xs) = do

    ------- increment processed line count
    modify (\state -> state { totallines = (totallines state)+1 })

    if (not.null) x then do

        ------- check if prevline can be parsed correctly
        doProcess []

        ------- get current state & env
        env <- ask
        state <- get

        ------- if debug is set
        if debug env
            then liftIO . hPutStrLn stderr $ "--" ++ show (totallines state) ++ ": " ++ x
            else return ()

        ------- get current line
        case parse (parsec_line (csv env)) "" x of

            Right cols -> do

                let longcol = longestCol cols
                if longcol > (longcolsize state)
                    then put state { longcolline = (totallines state), longcolsize = longcol }
                    else return ()

                ------- process current line
                if (all (\x -> length x <= (maxcolen env)) cols) && (length cols == colcount env)

                    ------- output clean lines: csv or sql ==> output line
                    then if (sql env)
                        then liftIO . hPutStrLn stdout $ outputSQL (sqltable env) cols
                        else liftIO . hPutStrLn stdout $ outputCsv (out env) cols  --x

                    ------- process dirty lines
                    else do

                        ------- check if delimiter count is unreliable
                        (cs,ds) <- asks csv
                        let corruptCol = splitStr [ds,cs,ds] (tail . init $ x)
                        if length corruptCol == colcount env

                            ------- column count is correct, delimiter is unreliable ==> output line
                            then liftIO . hPutStrLn stderr $ outputSQL (sqltable env) corruptCol

                            ------- column count is wrong : truncated line ==> save line into (prevline state)
                            else do
                                let xfilt = filter (/='\r') . filter (/='\n') $ x
                                put state { prevline = (prevline state) ++ xfilt }

            Left _ -> do
                let xfilt = filter (/='\r') . filter (/='\n') $ x
                put state { prevline = (prevline state) ++ xfilt }

    else do
        state <- get
        liftIO . hPutStrLn stderr $ "--Empty line on " ++ show (totallines state)

    doProcess xs
----------------------------------------------------------------------------



-- reader monad environment
----------------------------------------------------------------------------
data Env = Env
    { csv      :: (Char,Char)
    , out      :: (Char,Char)
    , maxcolen :: Int
    , sql      :: Bool
    , sqltable :: String
    , colcount :: Int
    , debug    :: Bool
    } deriving Show
----------------------------------------------------------------------------



-- state monad environment
----------------------------------------------------------------------------
data ProcState = ProcState
    { totallines  :: Int
    , longcolline :: Int
    , longcolsize :: Int
    , prevline    :: String
    } deriving Show
----------------------------------------------------------------------------



-- table name command line argument
----------------------------------------------------------------------------
argProcess :: String -> [String] -> String
argProcess cmd args = let
    wordsWhen p s =  case dropWhile p s of
        "" -> []
        s' -> w : wordsWhen p s''
            where (w, s'') = break p s'
    table_name = filter (isPrefixOf cmd) args
    file_arg   = listToMaybe . filter (\x -> head x /= '-') $ args
    file_name  = map toUpper . head . wordsWhen (=='.') . fromMaybe "--X--" $ file_arg
    in case table_name of
        [] -> file_name
        i  -> drop 2 $ head i
----------------------------------------------------------------------------



-- MAIN
----------------------------------------------------------------------------
main :: IO ()
main = do
    hSetBuffering stdout . BlockBuffering . Just $ 1024*10
    hSetBuffering stderr . BlockBuffering . Just $ 1024*10
    hSetEncoding stdin  utf8_bom
    hSetEncoding stdout utf8
    hSetEncoding stderr utf8

    args <- getArgs

    if length args == 0
        then putStrLn "CsvParser [-d|input_file] -t[table_name] [-sql] [--debug] 1>[cleaned_output] 2>[errors]"
        else do

            start <- getCurrentTime

            contents <- getInputFileContent args
            let contentLines = lines contents

            ------ reader monad
            let initialenv = Env {
                csv      = checkCsv (head contentLines),
                out      = ('|','~'),
                --out = (',','"'),
                maxcolen = 3000,
                sql      = elem "-sql" args,
                sqltable = argProcess "-t" args,
                colcount = 0,
                debug    = elem "--debug" args
            }

            let headercols = parseCsv (csv initialenv) (head contentLines)
            let env = initialenv { colcount = (length headercols) }

            hPutStrLn stderr $ outputCreateTable (sqltable env) headercols
            hPutStrLn stderr "-------------------------------"

            ------- main process
            let state = ProcState {
                totallines  = 1, -- line count includes header line
                longcolline = 0,
                longcolsize = 0,
                prevline    = ""
            }
            finalState <- flip runReaderT env . flip evalStateT state . doProcess $ tail contentLines

            hPutStrLn stderr "-------------------------------"
            hPutStrLn stderr $ printState finalState

            stop <- getCurrentTime

            hPutStrLn stderr $ "--Columns: "        ++ show (colcount env)
            hPutStrLn stderr $ "--Env setting: "    ++ show env
            hPutStrLn stderr $ "--Execution time: " ++ show (diffUTCTime stop start)
----------------------------------------------------------------------------
